#!/usr/bin/env python3
import json
import logging
import secrets

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.pebble import PathError, ProtocolError

from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
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
    """A Juju Charm to deploy MongoDB on Kubernetes.

    This charm has the following features:
    - Add one more MongoDB units
    - Reconfigure replica set anytime number of MongoDB units changes
    - Provides a database relation for any MongoDB client
    """
    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._stored.set_default(mongodb_initialized=False)

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

        if self._stored.mongodb_initialized and self.mongo.version:
            self.mongo_provider = MongoProvider(self, 'database')
        else:
            logger.debug("Mongo Provider not yet Available")

    ##############################################
    #           CHARM HOOKS HANDLERS             #
    ##############################################

    # Handles config-changed and upgrade-charm events
    def _on_config_changed(self, event):
        """(Re)Configure MongoDB pebble layer specification.

        A new MongoDB pebble layer specification is set only if it is
        different from the current specification.
        """
        logger.debug("Running config changed handler")
        container = self.unit.get_container("mongodb")

        if not container.can_connect():
            self.unit.status = WaitingStatus("Waiting for Pebble ready")
            return

        # Set security key
        if not self.have_security_key(container):
            self.set_security_key(container)

        # Build layer
        layers = MongoLayers(self.config)
        mongo_layer = layers.build()
        plan = container.get_plan()
        service_changed = plan.services != mongo_layer.services
        if service_changed:
            container.add_layer("mongodb", mongo_layer, combine=True)

        if service_changed or self.need_replica_set_reconfiguration:
            self.restart()
            logger.info("Restarted mongodb container")

        self.unit.status = ActiveStatus()

        self._on_update_status(event)
        logger.debug("Finished config changed handler")

    # Handles start event
    def _on_start(self, event):
        """Initialize MongoDB.

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

        if not self._stored.mongodb_initialized:
            self.unit.status = WaitingStatus("Initializing MongoDB")
            try:
                self.mongo.initialize_replica_set(self.mongo.cluster_hosts)
                self._stored.mongodb_initialized = True
                self.peers.data[self.app][
                    "replica_set_hosts"] = json.dumps(self.mongo.cluster_hosts)

                # Now that we've initialized MongoDB, we can finally set the necessary
                # relation data for the relations that were already created.
                self.mongo_provider = MongoProvider(self, 'database')
                self.mongo_provider.update_all_db_relations()
            except Exception as e:
                logger.info("Deferring on_start since : error={}".format(e))
                self._on_update_status(event)
                event.defer()
                return

        logger.debug("Running on_start finished")

    # Handles stop event
    def _on_stop(self, _):
        """Mark terminating unit as inactive.
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
            # TODO: remove container restarting here when Pebble
            # does this automatically if workload is inactive
            container = self.unit.get_container("mongodb")
            if container.can_connect() and "mongodb" in container.get_services():
                self.restart()
            return

        if not self._stored.mongodb_initialized:
            status_message = "mongodb not initialized"
            self.unit.status = WaitingStatus(status_message)
            return

        self.unit.status = ActiveStatus()

    ##############################################
    #        PEER RELATION HOOK HANDLERS         #
    ##############################################

    # Handles relation-changed and relation-departed events
    def _reconfigure(self, event):
        """Reconfigure replicat set.

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
        """Assume leadership.

        Each new leader checks if root password and the security key
        is available in peer relation data. If not the leader sets
        these into peer relation data.
        """
        data = self.peers.data[self.app]

        root_password = data.get('root_password', None)
        if root_password is None:
            self.peers.data[self.app]['root_password'] = str(self.root_password)

        security_key = data.get('security_key', None)
        if security_key is None:
            self.peers.data[self.app]['security_key'] = str(self.security_key)

    ##############################################
    #               PROPERTIES                   #
    ##############################################

    @property
    def need_replica_set_reconfiguration(self):
        """Does MongoDB replica set need reconfiguration.
        """
        return self.mongo.cluster_hosts != self.replica_set_hosts

    @property
    def replica_set_name(self):
        """Find the replica set name.
        """
        return self.model.config["replica_set_name"]

    @property
    def num_peers(self):
        """Find number of deployed MongoDB units.
        """
        return len(self.peers.units) + 1 if self.peers else 1

    @property
    def mongo(self):
        """Fetch the MongoDB server interface object.
        """
        return MongoDB(self.config)

    ##############################################
    #             UTILITY METHODS                #
    ##############################################

    @property
    def config(self):
        """Configuration for MongoDB replica set with authentication.
        """
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
        """MongoDB root password.
        """
        root_password = None
        peers = self.peers

        data = self.peers.data[peers.app]
        root_password = data.get('root_password', None)

        if root_password is None:
            root_password = MongoDB.new_password()

        return root_password

    @property
    def security_key(self):
        """Security key used for authentication replica set peers.
        """
        security_key = None

        data = self.peers.data[self.app]
        security_key = data.get('security_key', None)

        if security_key is None:
            security_key = secrets.token_hex(128)

        return security_key

    @property
    def peers(self):
        """Fetch the peer relation

        Returns:
             A :class:`ops.model.Relation` object representing
             the peer relation.
        """
        return self.model.get_relation(PEER)

    def replica_set_hosts(self):
        """Fetch current list of hosts in the replica set.

        Returns:
            A list of hosts addresses (strings).
        """
        hosts = json.loads(self.peers.data[self.app].get("replica_set_hosts", "[]"))
        return hosts

    def restart(self):
        """Shutdown MongoDB workload and restart container.
        """
        self.mongo.shutdown()
        container = self.unit.get_container("mongodb")
        container.restart("mongodb")

    def have_security_key(self, container):
        """Has the security key been uploaded to a workload container.

        Args:
            container: the container object that must be checked.

        Returns:
            True if container has security key, False otherwise.

        Raises:
            :class:`ops.pebble.ConnectionError` if pebble is not ready
        """
        file_path = SECRET_PATH + "/" + KEY_FILE
        try:
            key_file = container.pull(file_path)
            key = key_file.read()
        except PathError:
            return False
        return True if key else False

    def set_security_key(self, container):
        """Upload the security key to a workload container.

        Args:
            container: the container object that must be checked.

        Raises:
            :class:`ops.pebble.ConnectionError` if pebble is not ready
        """
        file_path = SECRET_PATH + "/" + KEY_FILE
        try:
            container.push(file_path,
                           self.security_key,
                           permissions=0o400,
                           user="mongodb",
                           group="mongodb")
        except (PathError, ProtocolError):
            self.unit.status = BlockedStatus("Failed to set security key")


if __name__ == "__main__":
    main(MongoDBCharm, use_juju_for_storage=True)
