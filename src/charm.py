#!/usr/bin/env python3

import logging

from urllib.parse import urlparse

from ops.charm import CharmBase, CharmEvents

from ops.framework import StoredState, EventBase, EventSource

from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    WaitingStatus,
)
from oci_image import OCIImageResource, OCIImageResourceError

from pod_spec import PodSpecBuilder
from cluster import MongoDBCluster
from mongo import Mongo

logger = logging.getLogger(__name__)

REQUIRED_SETTINGS = ["standalone"]
REQUIRED_SETTINGS_NOT_STANDALONE = ["replica_set_name"]

# We expect the mongodb container to use the
# default ports
MONGODB_PORT = 27017


class MongoDBStartedEvent(EventBase):
    pass


class MongoDBReadyEvent(EventBase):
    pass


class ReplicaSetConfigured(EventBase):
    def __init__(self, handle, hosts):
        super().__init__(handle)
        self.hosts = hosts

    def snapshot(self):
        return {"hosts": self.hosts}

    def restore(self, snapshot):
        self.hosts = snapshot["hosts"]


class MongoDBClusterEvents(CharmEvents):
    cluster_ready = EventSource(MongoDBReadyEvent)
    replica_set_configured = EventSource(ReplicaSetConfigured)


class MongoDBCharm(CharmBase):
    state = StoredState()
    on = MongoDBClusterEvents()

    def __init__(self, *args):
        super().__init__(*args)

        self.state.set_default(started=False)
        self.state.set_default(pod_spec=None)

        self.port = MONGODB_PORT
        self.image = OCIImageResource(self, "mongodb-image")

        # Register all of the events we want to observe
        self.framework.observe(self.on.install, self.configure_pod)
        self.framework.observe(self.on.config_changed, self.configure_pod)
        self.framework.observe(self.on.upgrade_charm, self.configure_pod)
        self.framework.observe(self.on.start, self.on_start)
        self.framework.observe(self.on.update_status, self.on_update_status)

        # Peer relation
        self.cluster = MongoDBCluster(self, "cluster", self.port)

        self.framework.observe(self.on.cluster_relation_changed, self.reconfigure)
        self.framework.observe(self.on.cluster_relation_departed, self.reconfigure)

        logger.debug("MongoDBCharm initialized!")

    ##############################################
    ########### CHARM HOOKS HANDLERS #############
    ##############################################

    # hooks: install, config-changed, upgrade-charm
    def configure_pod(self, event):
        # Continue only if the unit is the leader
        if not self.unit.is_leader():
            self.on_update_status(event)
            return

        logger.debug("Running configuring_pod")
        # Check problems in the settings
        problems = self._check_settings()
        if problems:
            self.unit.status = BlockedStatus(problems)
            return

        # Fetch image information
        try:
            self.unit.status = WaitingStatus("Fetching image information")
            image_info = self.image.fetch()
        except OCIImageResourceError:
            self.unit.status = BlockedStatus("Error fetching image information")
            return

        # Build Pod spec
        self.unit.status = WaitingStatus("Assembling pod spec")
        builder = PodSpecBuilder(
            name=self.model.app.name,
            replica_set_name=self.replica_set_name,
            port=self.port,
            image_info=image_info,
        )
        pod_spec = builder.make_pod_spec()

        # Update pod spec if the generated one is different
        # from the one previously applied
        if self.state.pod_spec != pod_spec:
            self.model.pod.set_spec(pod_spec)
            self.state.pod_spec = pod_spec

        self.on_update_status(event)
        logger.debug("Running configuring_pod finished")

    # hooks: start
    def on_start(self, event):
        if not self.unit.is_leader():
            return
        logger.debug("Running on_start")
        if self._is_mongodb_service_ready():
            self._initialize_mongodb_cluster(event)
        else:
            # This event is not being retriggered before update_status
            event.defer()

        self.on_update_status(event)
        logger.debug("Running on_start finished")

    # hooks: update-status
    def on_update_status(self, event):
        status_message = ""
        if self.standalone:
            status_message += "standalone-mode: "
            if self._is_mongodb_service_ready():
                status_message += "ready"
                self.unit.status = ActiveStatus(status_message)
            else:
                status_message += "service not ready yet"
                self.unit.status = WaitingStatus(status_message)
        else:
            status_message += f"replica-set-mode({self.replica_set_name}): "
            if self._is_mongodb_service_ready():
                status_message += "ready"
                if self.unit.is_leader():
                    if self.cluster.ready:
                        hosts_count = len(self.cluster.replica_set_hosts)
                        status_message += f" ({hosts_count} members)"
                    else:
                        status_message += " (replica set not ready yet)"
                        # Since on_start is not being properly triggered, I'm calling it manually here
                        self.on_start(event)
                self.unit.status = ActiveStatus(status_message)
            else:
                status_message += "service not ready yet"
                self.unit.status = WaitingStatus(status_message)

    ##############################################
    ######## PEER RELATION HOOK HANDLERS #########
    ##############################################

    # hooks: cluster-relation-changed, cluster-relation-departed
    def reconfigure(self, event):
        logger.debug("Running reconfigure")

        if (
            self.unit.is_leader()
            and self.cluster.replica_set_initialized
            and self.cluster.need_replica_set_reconfiguration()
        ):
            self.mongo.reconfigure_replica_set(self.cluster.hosts)
            self.on.replica_set_configured.emit(self.cluster.hosts)
        self.on_update_status(event)
        logger.debug("Running reconfigure finished")

    ##############################################
    ########## CLUSTER EVENT HANDLERS ############
    ##############################################

    def _initialize_mongodb_cluster(self, event):
        if not self.unit.is_leader() or self.standalone:
            self.on_update_status(event)
            return
        logger.debug("Initializing MongoDB Cluster")
        if not self.cluster.replica_set_initialized:
            self.unit.status = WaitingStatus("Initializing the replica set")
            self.mongo.initialize_replica_set(self.cluster.hosts)
            self.on.replica_set_configured.emit(self.cluster.hosts)

        self.on.cluster_ready.emit()
        self.on_update_status(event)
        logger.debug("MongoDB Cluster Initialized")

    ##############################################
    ############### PROPERTIES ###################
    ##############################################

    @property
    def mongo(self):
        return Mongo(
            standalone_uri=self.cluster.standalone_uri,
            replica_set_uri=f"{self.cluster.replica_set_uri}?replicaSet={self.replica_set_name}",
        )

    @property
    def replica_set_name(self):
        return self.model.config["replica_set_name"]

    @property
    def standalone(self):
        return self.model.config["standalone"]

    ##############################################
    ############## PRIVATE METHODS ###############
    ##############################################

    def _check_settings(self):
        problems = []
        config = self.model.config

        for setting in REQUIRED_SETTINGS:
            if config.get(setting) is None:
                problem = f"missing config {setting}"
                problems.append(problem)
        if not self.standalone:
            for setting in REQUIRED_SETTINGS_NOT_STANDALONE:
                if not config.get(setting):
                    problem = f"missing config {setting}"
                    problems.append(problem)

        return ";".join(problems)

    def _is_mongodb_service_ready(self):
        return self.mongo.is_ready()


if __name__ == "__main__":
    main(MongoDBCharm)
