#!/usr/bin/env python3
import logging
import secrets

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.pebble import PathError
from subprocess import check_output

from ops.main import main
from ops.model import (
    ActiveStatus,
    WaitingStatus,
    MaintenanceStatus
)

from pebble_layers import MongoLayers, SECRET_PATH, KEY_FILE
from mongoserver import MongoDB, MONGODB_PORT
from mongoprovider import MongoProvider

logger = logging.getLogger(__name__)

# Name of peer relation
PEER = "mongodb"


class MongoDBCharm(CharmBase):
    """A Juju Charm to deploy MongoDB on Kubernetes

    This charm has the following features:
    - Add one more MongoDB units
    - Reconfigure replica set anytime number of MongoDB units changes
    - Provides a database relation for any MongoDB client
    """
    state = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self.state.set_default(mongodb_initialized=False)
        self.state.set_default(replica_set_hosts=None)

        self.port = MONGODB_PORT

        # Register all of the events we want to observe
        self.framework.observe(self.on.mongodb_pebble_ready, self._on_config_changed)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.stop, self._on_stop)
        self.framework.observe(self.on.update_status, self._on_update_status)

        self.framework.observe(self.on[PEER].relation_changed,
                               self._reconfigure)
        self.framework.observe(self.on[PEER].relation_departed,
                               self._reconfigure)

        self.framework.observe(self.on.leader_elected,
                               self._on_leader_elected)

        if self.state.mongodb_initialized and self.mongo.version:
            self.mongo_provider = MongoProvider(self,
                                                'database',
                                                'mongodb',
                                                version=self.mongo.version)
            self.mongo_provider.ready()
        else:
            logger.debug("Mongo Provider not yet Available")

    ##############################################
    #           CHARM HOOKS HANDLERS             #
    ##############################################

    # Handles config-changed and upgrade-charm events
    def _on_config_changed(self, event):
        """(Re)Configure MongoDB pebble layer specification

        A new MongoDB pebble layer specification is set only if it is
        different from the current specification.
        """
        logger.debug("Running config changed handler")
        container = self.unit.get_container("mongodb")

        # Set security key
        if not self.have_security_key(container):
            self.set_security_key(container)

        # Build layer
        layers = MongoLayers(self.config)
        mongo_layer = layers.build()
        plan = container.get_plan()
        if plan.services != mongo_layer["services"]:
            container.add_layer("mongodb", mongo_layer, combine=True)

            if container.get_service("mongodb").is_running():
                container.stop("mongodb")

            container.start("mongodb")
            logger.info("Restarted mongodb container")

        self.unit.status = ActiveStatus()

        self._on_update_status(event)
        logger.debug("Finished config changed handler")

    # Handles start event
    def _on_start(self, event):
        """Initialize MongoDB

        This event handler is deferred if initialization of MongoDB
        replica set fails. By doing so it is gauranteed that another
        attempt at initialization will be made.
        """
        logger.debug("Running on_start")
        if not self.unit.is_leader():
            return

        if not self.mongo.is_ready():
            self.unit.status = WaitingStatus("Waiting for MongoDB Service")
            self._on_update_status(event)
            event.defer()
            return

        if not self.state.mongodb_initialized:
            self.unit.status = WaitingStatus("Initializing MongoDB")
            try:
                self.mongo.initialize_replica_set(self.mongo.cluster_hosts)
                self.state.mongodb_initialized = True
                self.state.replica_set_hosts = self.mongo.cluster_hosts
            except Exception as e:
                logger.info("Deferring on_start since : error={}".format(e))
                self._on_update_status(event)
                event.defer()
                return

        logger.debug("Running on_start finished")

    # Handles stop event
    def _on_stop(self, _):
        """Mark terminating unit as inactive
        """
        self.unit.status = MaintenanceStatus('Pod is terminating.')

    # Handles update-status event
    def _on_update_status(self, event):
        """Set status for all units

        Status may be
        - MongoDB API server not reachable (service is not ready),
        - MongoDB Replication set is not Initialized
        - Unit is active
        """
        if not self.unit.is_leader():
            self.unit.status = ActiveStatus()
            return

        if not self.mongo.is_ready():
            status_message = "service not ready yet"
            self.unit.status = WaitingStatus(status_message)
            return

        if not self.state.mongodb_initialized:
            status_message = "mongodb not initialized"
            self.unit.status = WaitingStatus(status_message)
            return

        self.unit.status = ActiveStatus()

    ##############################################
    #        PEER RELATION HOOK HANDLERS         #
    ##############################################

    # Handles relation-changed and relation-departed events
    def _reconfigure(self, event):
        """Reconfigure replicat set

        The number of replicas in the MongoDB replica set is updated.
        """
        logger.debug("Running reconfigure")

        if not self.unit.is_leader():
            self._on_update_status(event)
            return

        if self.need_replica_set_reconfiguration:
            try:
                self.mongo.reconfigure_replica_set(self.mongo.cluster_hosts)
            except Exception as e:
                logger.info("Deferring reconfigure since : error={}".format(e))
                event.defer()

        self._on_update_status(event)
        logger.debug("Running reconfigure finished")

    def _on_leader_elected(self, event):
        peers = self.framework.model.get_relation(PEER)

        if peers:
            data = peers.data[peers.app]

            root_password = data.get('root_password', None)
            if root_password is None:
                peers.data[peers.app]['root_password'] = str(self.root_password)

            security_key = data.get('security_key', None)
            if security_key is None:
                peers.data[peers.app]['security_key'] = str(self.security_key)

    ##############################################
    #               PROPERTIES                   #
    ##############################################

    @property
    def need_replica_set_reconfiguration(self):
        """Does MongoDB replica set need reconfiguration
        """
        return self.mongo.cluster_hosts != self.state.replica_set_hosts

    @property
    def replica_set_name(self):
        """Find the replica set name
        """
        return self.model.config["replica_set_name"]

    @property
    def num_peers(self):
        """Find number of deployed MongoDB units
        """
        peer_relation = self.framework.model.get_relation(PEER)
        return len(peer_relation.units) + 1 if self.is_joined else 1

    @property
    def is_joined(self):
        """Does MongoDB charm have peers
        """
        peer_relation = self.framework.model.get_relation(PEER)
        return peer_relation is not None

    @property
    def mongo(self):
        return MongoDB(self.config)

    ##############################################
    #             UTILITY METHODS                #
    ##############################################

    @property
    def config(self):
        config = {
            "app_name": self.model.app.name,
            "replica_set_name": self.replica_set_name,
            "num_peers": self.num_peers,
            "port": self.port,
            "root_password": self.root_password,
            "security_key": self.security_key
        }
        return config

    @property
    def root_password(self):
        root_password = None
        peers = self.framework.model.get_relation(PEER)

        if peers:
            data = peers.data[peers.app]
            root_password = data.get('root_password', None)

        if root_password is None:
            root_password = MongoDB.new_password()

        return root_password

    @property
    def security_key(self):
        security_key = None
        peers = self.framework.model.get_relation(PEER)

        if peers:
            data = peers.data[peers.app]
            security_key = data.get('security_key', None)

        if security_key is None:
            security_key = secrets.token_hex(128)

        return security_key

    @property
    def ip(self):
        ip = check_output(["unit-get", "private-address"]).decode().strip()
        return ip

    def have_security_key(self, container):
        file_path = SECRET_PATH + "/" + KEY_FILE
        try:
            key_file = container.pull(file_path)
            key = key_file.read()
        except PathError:
            return False
        return True if key else False

    def set_security_key(self, container):
        file_path = SECRET_PATH + "/" + KEY_FILE
        container.push(file_path,
                       self.security_key,
                       permissions=0o400,
                       user="mongodb",
                       group="mongodb")


if __name__ == "__main__":
    main(MongoDBCharm, use_juju_for_storage=True)
