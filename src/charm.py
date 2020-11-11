#!/usr/bin/env python3

import logging
import json

from ops.charm import CharmBase
from ops.framework import StoredState

from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    WaitingStatus,
)
from oci_image import OCIImageResource, OCIImageResourceError

from pod_spec import PodSpecBuilder
from mongo import Mongo

logger = logging.getLogger(__name__)

REQUIRED_SETTINGS = ["standalone"]
REQUIRED_SETTINGS_NOT_STANDALONE = ["replica_set_name"]

# We expect the mongodb container to use the
# default ports
MONGODB_PORT = 27017

# Name of peer relation
PEER = "cluster"


class MongoDBCharm(CharmBase):
    state = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self.state.set_default(pod_spec=None)
        self.state.set_default(cluster_initialized=None)
        self.state.set_default(replica_set_hosts=None)

        self.peer_relation = self.framework.model.get_relation(PEER)

        self.port = MONGODB_PORT
        self.image = OCIImageResource(self, "mongodb-image")

        # Register all of the events we want to observe
        self.framework.observe(self.on.config_changed, self.configure_pod)
        self.framework.observe(self.on.upgrade_charm, self.configure_pod)
        self.framework.observe(self.on.start, self.on_start)
        self.framework.observe(self.on.update_status, self.on_update_status)

        self.framework.observe(self.on["database"].relation_changed,
                               self.on_database_relation_changed)
        self.framework.observe(self.on[PEER].relation_changed,
                               self.reconfigure)
        self.framework.observe(self.on[PEER].relation_departed,
                               self.reconfigure)

        logger.debug("MongoDBCharm initialized!")

    ##############################################
    #           CHARM HOOKS HANDLERS             #
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
            self.unit.status = BlockedStatus(
                "Error fetching image information")
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
                    if self.state.cluster_initialized:
                        hosts_count = len(self.replica_set_hosts)
                        status_message += f" ({hosts_count} members)"
                self.unit.status = ActiveStatus(status_message)
            else:
                status_message += "service not ready yet"
                self.unit.status = WaitingStatus(status_message)

    ##############################################
    #        PEER RELATION HOOK HANDLERS         #
    ##############################################

    # hooks: cluster-relation-changed, cluster-relation-departed
    def reconfigure(self, event):
        logger.debug("Running reconfigure")

        if (
            self.unit.is_leader()
            and self.replica_set_initialized
            and self.need_replica_set_reconfiguration()
        ):
            self.mongo.reconfigure_replica_set(self.cluster_hosts)
            self.update_replica_set_status(self.cluster_hosts)
        self.on_update_status(event)
        logger.debug("Running reconfigure finished")

    ##############################################
    #          CLUSTER EVENT HANDLERS            #
    ##############################################

    def _initialize_mongodb_cluster(self, event):
        if not self.unit.is_leader() or self.standalone:
            self.on_update_status(event)
            return
        logger.debug("Initializing MongoDB Cluster")
        if not self.replica_set_initialized:
            self.unit.status = WaitingStatus("Initializing the replica set")
            self.mongo.initialize_replica_set(self.cluster_hosts)
            self.update_replica_set_status(self.cluster_hosts)

        self.update_cluster_status(event)
        self.on_update_status(event)
        logger.debug("MongoDB Cluster Initialized")

    def update_cluster_status(self, event):
        if not self.framework.model.unit.is_leader():
            return

        self.state.cluster_initialized = True

        if not self.is_joined:
            logger.debug("cluster status: No relation joined yet")
            event.defer()
            return

        self.peer_relation.data[self.model.app][
            "initialized"] = str(self.state.cluster_initialized)

        logger.debug(f"Relation data updated: initialized="
                     "{self.state.cluster_initialized}")

    def update_replica_set_status(self, hosts):
        if not self.framework.model.unit.is_leader():
            return

        self.state.replica_set_hosts = hosts

        if not self.is_joined:
            logger.debug("replica set status: No relation joined yet")
            return

        replica_set_hosts = str(self.state.replica_set_hosts)
        self.peer_relation.data[self.model.app][
            "replica_set_hosts"] = replica_set_hosts

        logger.debug(
            f"Relation data updated: replica_set_hosts={replica_set_hosts}"
        )

    ##############################################
    #               RELATIONS                    #
    ##############################################

    def on_database_relation_changed(self, event):
        event.relation.data[self.unit]['replicated'] = str(self.is_joined)
        event.relation.data[self.unit][
            'replica_set_name'] = self.replica_set_name
        event.relation.data[self.unit]['standalone_uri'] = "{}".format(
            self.standalone_uri)
        event.relation.data[self.unit]['replica_set_uri'] = "{}".format(
            self.replica_set_uri)

    ##############################################
    #               PROPERTIES                   #
    ##############################################

    @property
    def mongo(self):
        return Mongo(
            standalone_uri=self.standalone_uri,
            replica_set_uri=f"{self.replica_set_uri}?replicaSet={self.replica_set_name}",
        )

    @property
    def replica_set_uri(self):
        uri = "mongodb://"
        for i, host in enumerate(self.cluster_hosts):
            if i:
                uri += ","
            uri += f"{host}:{self.port}"
        uri += "/"
        return uri

    @property
    def standalone_uri(self):
        return f"mongodb://{self.model.app.name}:{self.port}/"

    @property
    def replica_set_name(self):
        return self.model.config["replica_set_name"]

    @property
    def num_peers(self):
        return len(self.peer_relation.units) + 1 if self.is_joined else 1
    
    @property
    def standalone(self):
        return self.model.config["standalone"]

    @property
    def is_joined(self):
        return self.peer_relation is not None

    @property
    def replica_set_initialized(self):
        return self.replica_set_hosts is not None

    def _get_unit_hostname(self, _id: int) -> str:
        return f"{self.model.app.name}-{_id}.{self.model.app.name}-endpoints"

    @property
    def cluster_hosts(self: int) -> list:
        return [self._get_unit_hostname(i) for i in range(self.num_peers)]

    @property
    def replica_set_hosts(self):

        if not self.state.replica_set_hosts and self.is_joined:
            hosts = self.peer_relation.data[self.model.app].get(
                "replica_set_hosts")
            if hosts:
                self.state.replica_set_hosts = json.loads(hosts)
        return self.state.replica_set_hosts

    def need_replica_set_reconfiguration(self):
        return self.cluster_hosts != self.replica_set_hosts

    ##############################################
    #              PRIVATE METHODS               #
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
