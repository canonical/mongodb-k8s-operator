#!/usr/bin/env python3
import logging
import secrets
import string

from ops.charm import CharmBase
from ops.framework import StoredState

from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    WaitingStatus,
    MaintenanceStatus
)
from oci_image import OCIImageResource, OCIImageResourceError

from pod_spec import PodSpecBuilder
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
        self.state.set_default(root_password=None)
        self.state.set_default(security_key=None)

        self.port = MONGODB_PORT
        self.image = OCIImageResource(self, "mongodb-image")

        # Register all of the events we want to observe
        self.framework.observe(self.on.config_changed, self.configure_pod)
        self.framework.observe(self.on.upgrade_charm, self.configure_pod)
        self.framework.observe(self.on.start, self.on_start)
        self.framework.observe(self.on.stop, self.on_stop)
        self.framework.observe(self.on.update_status, self.on_update_status)

        self.framework.observe(self.on[PEER].relation_changed,
                               self.reconfigure)
        self.framework.observe(self.on[PEER].relation_departed,
                               self.reconfigure)

        self.framework.observe(self.on.leader_elected,
                               self.on_leader_elected)

        if self.state.mongodb_initialized:
            logger.debug("Mongo Provider Available")
            self.mongo_provider = MongoProvider(self, 'database', self.provides)
            self.mongo_provider.ready()
        else:
            logger.debug("Mongo Provider not yet Available")

        logger.debug("MongoDBCharm initialized!")

    ##############################################
    #           CHARM HOOKS HANDLERS             #
    ##############################################

    # Handles config-changed and upgrade-charm events
    def configure_pod(self, event):
        """Configure MongoDB Pod specification

        A new MongoDB pod specification is set only if it is different
        from the current specification.
        """
        # Continue only if the unit is the leader
        if not self.unit.is_leader():
            self.on_update_status(event)
            return

        logger.debug("Running configuring_pod")

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
        builder_config = {
            "name": self.model.app.name,
            "replica_set_name": self.replica_set_name,
            "port": self.port,
            "image_info": image_info,
            "root_password": self.root_password,
            "security_key": self.security_key
        }
        builder = PodSpecBuilder(builder_config)
        pod_spec = builder.make_pod_spec()

        # Applying pod spec. If the spec hasn't changed, this has no effect.
        self.model.pod.set_spec(pod_spec)

        self.on_update_status(event)
        logger.debug("Running configuring_pod finished")

    # Handles start event
    def on_start(self, event):
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
            logger.debug("Waiting for MongoDB Service")
            event.defer()

        if not self.state.mongodb_initialized:
            logger.debug("Initializing MongoDB")
            self.unit.status = WaitingStatus("Initializing MongoDB")
            try:
                self.mongo.initialize_replica_set(self.mongo.cluster_hosts)
                self.state.mongodb_initialized = True
                self.state.replica_set_hosts = self.mongo.cluster_hosts
                logger.debug("MongoDB Initialized")
            except Exception as e:
                logger.info("Deferring on_start since : error={}".format(e))
                event.defer()

        self.on_update_status(event)
        logger.debug("Running on_start finished")

    # Handles stop event
    def on_stop(self, _):
        """Mark terminating unit as inactive
        """
        self.unit.status = MaintenanceStatus('Pod is terminating.')

    # Handles update-status event
    def on_update_status(self, event):
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
    def reconfigure(self, event):
        """Reconfigure replicat set

        The number of replicas in the MongoDB replica set is updated.
        """
        logger.debug("Running reconfigure")

        if self.unit.is_leader():
            event.relation.data[event.app]['root_password'] = str(self.state.root_password)
            event.relation.data[event.app]['security_key'] = str(self.state.security_key)

            if self.need_replica_set_reconfiguration:
                try:
                    self.mongo.reconfigure_replica_set(self.mongo.cluster_hosts)
                except Exception as e:
                    logger.info("Deferring relation event since : error={}".format(e))
                    event.defer()

        self.on_update_status(event)
        logger.debug("Running reconfigure finished")

    def on_leader_elected(self, event):
        rel = self.framework.model.get_relation(PEER)
        if rel:
            data = rel.data[rel.app]
            root_password = data.get('root_password', None)
            self.state.root_password = root_password
            security_key = data.get('security_key', None)
            self.state.security_key = security_key

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
    def db_info(self):
        info = {
            'replicated': str(self.is_joined),
            'replica_set_name': self.replica_set_name,
            'standalone_uri': "{}".format(self.mongo.standalone_uri),
            'replica_set_uri': "{}".format(self.mongo.replica_set_uri)
        }
        return info

    @property
    def mongo(self):
        config = {'app_name': self.model.app.name,
                  'replica_set_name': self.replica_set_name,
                  'num_peers': self.num_peers,
                  'port': self.port,
                  'root_password': self.root_password}
        return MongoDB(config)

    ##############################################
    #             UTILITY METHODS                #
    ##############################################

    @property
    def provides(self):
        provided = {
            "provides": {"mongodb": str(self.mongo.version)},
            "config": self.db_info
        }
        logger.debug("Providing : {}".format(provided))
        return provided

    @property
    def root_password(self):
        if self.state.root_password is None:
            choices = string.ascii_letters + string.digits
            pwd = "".join([secrets.choice(choices) for i in range(16)])
            self.state.root_password = pwd
            return pwd
        else:
            return self.state.root_password

    @property
    def security_key(self):
        if self.state.security_key is None:
            key = secrets.token_hex(128)
            self.state.security_key = key
            return key
        else:
            return self.state.security_key


if __name__ == "__main__":
    main(MongoDBCharm)
