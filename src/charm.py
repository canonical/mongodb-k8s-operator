#!/usr/bin/env python3
"""Charm code for MongoDB service on Kubernetes."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import jinja2
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v0.loki_push_api import LogProxyConsumer
from charms.mongodb.v0.config_server_interface import ClusterProvider
from charms.mongodb.v0.mongodb_secrets import SecretCache, generate_secret_label
from charms.mongodb.v0.set_status import MongoDBStatusHandler
from charms.mongodb.v0.upgrade_helpers import (
    FailedToElectNewPrimaryError,
    UnitState,
    unit_number,
)
from charms.mongodb.v1.helpers import (
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    get_mongod_args,
    get_mongos_args,
)
from charms.mongodb.v1.mongodb import (
    MongoConfiguration,
    MongoDBConnection,
    NotReadyError,
)
from charms.mongodb.v1.mongodb_backups import MongoDBBackups
from charms.mongodb.v1.mongodb_provider import MongoDBProvider
from charms.mongodb.v1.mongodb_tls import MongoDBTLS
from charms.mongodb.v1.shards_interface import ConfigServerRequirer, ShardingProvider
from charms.mongodb.v1.users import (
    CHARM_USERS,
    BackupUser,
    MongoDBUser,
    MonitorUser,
    OperatorUser,
)
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from data_platform_helpers.version_check import (
    CrossAppVersionChecker,
    get_charm_revision,
)
from ops.charm import (
    ActionEvent,
    CharmBase,
    ConfigChangedEvent,
    RelationDepartedEvent,
    RelationEvent,
    StartEvent,
    UpdateStatusEvent,
    UpgradeCharmEvent,
)
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    Container,
    MaintenanceStatus,
    ModelError,
    Relation,
    RelationDataContent,
    Unit,
    WaitingStatus,
)
from ops.pebble import ChangeError, ExecError, Layer, PathError, ProtocolError
from pymongo.errors import PyMongoError
from tenacity import (
    RetryError,
    Retrying,
    before_log,
    retry,
    stop_after_attempt,
    wait_fixed,
)

import k8s_upgrade
from config import Config
from exceptions import (
    AdminUserCreationError,
    ContainerNotReadyError,
    MissingSecretError,
    NotConfigServerError,
)
from k8s_upgrade import MongoDBUpgrade

logger = logging.getLogger(__name__)

# Disable spamming logs from lightkube
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

UNIT_REMOVAL_TIMEOUT = 1000

APP_SCOPE = Config.Relations.APP_SCOPE
UNIT_SCOPE = Config.Relations.UNIT_SCOPE
Scopes = Config.Relations.Scopes

USER_CREATING_MAX_ATTEMPTS = 5
USER_CREATION_COOLDOWN = 30
REPLICA_SET_INIT_CHECK_TIMEOUT = 10


class MongoDBCharm(CharmBase):
    """A Juju Charm to deploy MongoDB on Kubernetes."""

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.mongod_pebble_ready, self._on_mongod_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.upgrade_charm, self._on_upgrade)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_joined, self._relation_changes_handler
        )

        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_changed, self._relation_changes_handler
        )

        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_departed, self._relation_changes_handler
        )

        # if a new leader has been elected update hosts of MongoDB
        self.framework.observe(self.on.leader_elected, self._relation_changes_handler)

        self.framework.observe(self.on.get_password_action, self._on_get_password)
        self.framework.observe(self.on.set_password_action, self._on_set_password)
        self.framework.observe(self.on.stop, self._on_stop)

        self.framework.observe(self.on.secret_remove, self._on_secret_remove)
        self.framework.observe(self.on.secret_changed, self._on_secret_changed)

        self.client_relations = MongoDBProvider(self, substrate=Config.SUBSTRATE)
        self.tls = MongoDBTLS(self, Config.Relations.PEERS, Config.SUBSTRATE)
        self.backups = MongoDBBackups(self)

        self.status = MongoDBStatusHandler(self)
        self.secrets = SecretCache(self)

        self.shard = ConfigServerRequirer(self)
        self.config_server = ShardingProvider(self)
        self.cluster = ClusterProvider(self)
        self.metrics_endpoint = MetricsEndpointProvider(
            self, refresh_event=[self.on.start, self.on.update_status], jobs=self.monitoring_jobs
        )
        self.grafana_dashboards = GrafanaDashboardProvider(self)
        self.loki_push = LogProxyConsumer(
            self,
            log_files=Config.get_logs_files_paths(),
            relation_name=Config.Relations.LOGGING,
            container_name=Config.CONTAINER_NAME,
        )
        self.status = MongoDBStatusHandler(self)
        self.secrets = SecretCache(self)

        self.shard = ConfigServerRequirer(self)
        self.config_server = ShardingProvider(self)
        self.cluster = ClusterProvider(self)
        self.upgrade = MongoDBUpgrade(self)

        self.version_checker = CrossAppVersionChecker(
            self,
            version=get_charm_revision(self.unit, local_version=self.get_charm_internal_revision),
            relations_to_check=[
                Config.Relations.SHARDING_RELATIONS_NAME,
                Config.Relations.CONFIG_SERVER_RELATIONS_NAME,
            ],
        )

    # BEGIN: properties

    @property
    def monitoring_jobs(self) -> list[dict[str, Any]]:
        """Defines the labels and targets for metrics."""
        return [
            {
                "static_configs": [
                    {
                        "targets": [f"*:{Config.Monitoring.MONGODB_EXPORTER_PORT}"],
                        "labels": {"cluster": self.get_config_server_name() or self.app.name},
                    }
                ]
            }
        ]

    @property
    def app_hosts(self) -> List[str]:
        """Retrieve IP addresses associated with MongoDB application.

        Returns:
            a list of IP address associated with MongoDB application.
        """
        self_unit = [self.unit_host(self.unit)]

        if not self._peers:
            return self_unit

        return self_unit + [self.unit_host(unit) for unit in self._peers.units]

    @property
    def _peers(self) -> Optional[Relation]:
        """Fetch the peer relation.

        Returns:
             An `ops.model.Relation` object representing the peer relation.
        """
        return self.model.get_relation(Config.Relations.PEERS)

    @property
    def peers_units(self) -> list[Unit]:
        """Get peers units in a safe way."""
        if not self._peers:
            return []
        else:
            return self._peers.units

    @property
    def mongo_config(self) -> MongoConfiguration:
        """Returns a MongoConfiguration object for shared libs with agnostic mongo commands."""
        return self.mongodb_config

    @property
    def mongodb_config(self) -> MongoConfiguration:
        """Create a configuration object with settings.

        Needed for correct handling interactions with MongoDB.

        Returns:
            A MongoConfiguration object
        """
        return self._get_mongodb_config_for_user(OperatorUser, self.app_hosts)

    @property
    def mongos_config(self) -> MongoConfiguration:
        """Generates a MongoConfiguration object for mongos in the deployment of MongoDB."""
        return self._get_mongos_config_for_user(OperatorUser, set(self.app_hosts))

    @property
    def monitor_config(self) -> MongoConfiguration:
        """Generates a MongoConfiguration object for this deployment of MongoDB."""
        return self._get_mongodb_config_for_user(MonitorUser, [self.unit_host(self.unit)])

    @property
    def backup_config(self) -> MongoConfiguration:
        """Generates a MongoConfiguration object for backup."""
        return self._get_mongodb_config_for_user(BackupUser, [self.unit_host(self.unit)])

    @property
    def _mongod_layer(self) -> Layer:
        """Returns a Pebble configuration layer for mongod."""
        layer_config = {
            "summary": "mongod layer",
            "description": "Pebble config layer for replicated mongod",
            "services": {
                "mongod": {
                    "override": "replace",
                    "summary": "mongod",
                    "command": "mongod " + get_mongod_args(self.mongodb_config, role=self.role),
                    "startup": "enabled",
                    "user": Config.UNIX_USER,
                    "group": Config.UNIX_GROUP,
                }
            },
        }
        return Layer(layer_config)

    @property
    def _mongos_layer(self) -> Layer:
        """Returns a Pebble configuration layer for mongos."""
        layer_config = {
            "summary": "mongos layer",
            "description": "Pebble config layer for mongos router",
            "services": {
                "mongos": {
                    "override": "replace",
                    "summary": "mongos",
                    "command": "mongos " + get_mongos_args(self.mongodb_config),
                    "startup": "enabled",
                    "user": Config.UNIX_USER,
                    "group": Config.UNIX_GROUP,
                }
            },
        }
        return Layer(layer_config)  # type: ignore

    @property
    def _monitor_layer(self) -> Layer:
        """Returns a Pebble configuration layer for mongodb_exporter."""
        layer_config = {
            "summary": "mongodb_exporter layer",
            "description": "Pebble config layer for mongodb_exporter",
            "services": {
                "mongodb_exporter": {
                    "override": "replace",
                    "summary": "mongodb_exporter",
                    "command": "mongodb_exporter --collector.diagnosticdata --compatible-mode",
                    "startup": "enabled",
                    "user": Config.UNIX_USER,
                    "group": Config.UNIX_GROUP,
                    "environment": {"MONGODB_URI": self.monitor_config.uri},
                }
            },
        }
        return Layer(layer_config)

    @property
    def _log_rotate_layer(self) -> Layer:
        """Returns a Pebble configuration layer for log rotate."""
        container = self.unit.get_container(Config.CONTAINER_NAME)
        # Before generating the log rotation layer we must first configure the log rotation
        # template.
        with open(Config.LogRotate.LOG_ROTATE_TEMPLATE, "r") as file:
            template = jinja2.Template(file.read())

        container.push(
            Config.LogRotate.RENDERED_TEMPLATE,
            template.render(
                logs_directory=Config.LOG_DIR,
                mongo_user=Config.UNIX_USER,
                max_log_size=Config.LogRotate.MAX_LOG_SIZE,
                max_rotations=Config.LogRotate.MAX_ROTATIONS_TO_KEEP,
            ),
        )

        layer_config = {
            "summary": "Log rotate layer",
            "description": "Pebble config layer for rotating mongodb logs",
            "services": {
                "logrotate": {
                    "summary": "log rotate",
                    # Pebble errors out if the command exits too fast (1s).
                    "command": f"sh -c 'logrotate {Config.LogRotate.RENDERED_TEMPLATE}; sleep 1'",
                    "startup": "enabled",
                    "override": "replace",
                    "backoff-delay": "1m0s",
                    "backoff-factor": 1,
                    "user": Config.UNIX_USER,
                    "group": Config.UNIX_GROUP,
                }
            },
        }
        return Layer(layer_config)

    @property
    def _backup_layer(self) -> Layer:
        """Returns a Pebble configuration layer for pbm."""
        layer_config = {
            "summary": "pbm layer",
            "description": "Pebble config layer for pbm",
            "services": {
                Config.Backup.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "pbm",
                    "command": "pbm-agent",
                    "startup": "enabled",
                    "user": Config.UNIX_USER,
                    "group": Config.UNIX_GROUP,
                    "environment": {"PBM_MONGODB_URI": self.backup_config.uri},
                }
            },
        }
        return Layer(layer_config)

    @property
    def relation(self) -> Optional[Relation]:
        """Peer relation data object."""
        return self.model.get_relation(Config.Relations.PEERS)

    @property
    def unit_peer_data(self) -> Dict:
        """Peer relation data object."""
        relation = self.relation
        if relation is None:
            return {}

        return relation.data[self.unit]

    @property
    def app_peer_data(self) -> RelationDataContent:
        """Peer relation data object."""
        relation = self.relation
        if relation is None:
            return {}

        return relation.data[self.app]

    @property
    def db_initialised(self) -> bool:
        """Check if MongoDB is initialised."""
        return json.loads(self.app_peer_data.get("db_initialised", "false"))

    @property
    def unit_departed(self) -> bool:
        """Whether the unit has departed or not."""
        return json.loads(self.unit_peer_data.get("unit_departed", "false"))

    @unit_departed.setter
    def unit_departed(self, value: bool) -> None:
        """Set the unit_departed flag."""
        if isinstance(value, bool):
            self.unit_peer_data["unit_departed"] = json.dumps(value)
        else:
            raise ValueError(
                f"'unit_departed' must be a boolean value. Provided: {value} is of type {type(value)}"
            )

    def is_role_changed(self) -> bool:
        """Checks if application is running in provided role."""
        return self.role != self.model.config["role"]

    def is_role(self, role_name: str) -> bool:
        """Checks if application is running in provided role."""
        return self.role == role_name

    def has_config_server(self) -> bool:
        """Returns True if we have a config-server."""
        return self.get_config_server_name() is not None

    def get_config_server_name(self) -> str | None:
        """Returns the name of the Juju Application that the shard is using as a config server."""
        if not self.is_role(Config.Role.SHARD):
            logger.info(
                "Component %s is not a shard, cannot be integrated to a config-server.", self.role
            )
            return None

        return self.shard.get_config_server_name()

    @db_initialised.setter
    def db_initialised(self, value):
        """Set the db_initialised flag."""
        if isinstance(value, bool):
            self.app_peer_data["db_initialised"] = json.dumps(value)
        else:
            raise ValueError(
                f"'db_initialised' must be a boolean value. Provided: {value} is of type {type(value)}"
            )

    @property
    def upgrade_in_progress(self) -> bool:
        """Returns true if upgrade is in progress."""
        if not self.upgrade._upgrade:
            return False
        return self.upgrade._upgrade.in_progress

    @property
    def replica_set_initialised(self) -> bool:
        """Check if the MongoDB replica set is initialised."""
        return json.loads(self.app_peer_data.get("replica_set_initialised", "false"))

    @replica_set_initialised.setter
    def replica_set_initialised(self, value):
        """Set the replica_set_initialised flag."""
        if isinstance(value, bool):
            self.app_peer_data["replica_set_initialised"] = json.dumps(value)
        else:
            raise ValueError(
                f"'replica_set_initialised' must be a boolean value. Proivded: {value} is of type {type(value)}"
            )

    @property
    def users_initialized(self) -> bool:
        """Check if MongoDB users are created."""
        return json.loads(self.app_peer_data.get("users_initialized", "false"))

    @users_initialized.setter
    def users_initialized(self, value):
        """Set the users_initialized flag."""
        if isinstance(value, bool):
            self.app_peer_data["users_initialized"] = json.dumps(value)
        else:
            raise ValueError(
                f"'users_initialized' must be a boolean value. Proivded: {value} is of type {type(value)}"
            )

    @property
    def role(self) -> str:
        """Returns role of MongoDB deployment.

        At any point in time the user can do juju config role=new-role, but this functionality is
        not yet supported in the charm. This means that we cannot trust what is in
        self.model.config["role"] since it can change at the users command. For this reason we save
        the role in the application data bag and only refer to that as the official role.
        """
        # case 1: official role has not been set in the application peer data bag
        if (
            "role" not in self.app_peer_data
            and self.unit.is_leader()
            and self.model.config["role"]
        ):
            self.app_peer_data["role"] = self.model.config["role"]
            # app data bag isn't set until function completes
            return self.model.config["role"]
        # case 2: leader hasn't set the role yet
        elif "role" not in self.app_peer_data:
            return self.model.config["role"]

        # case 3: the role is already set
        return self.app_peer_data.get("role")

    @property
    def drained(self) -> bool:
        """Returns whether the shard has been drained."""
        if not self.is_role(Config.Role.SHARD):
            logger.info("Component %s is not a shard, cannot check draining status.", self.role)
            return False

        return self.unit_peer_data.get("drained", False)

    @property
    def primary(self) -> str | None:
        """Retrieves the unit with the primary replica."""
        try:
            with MongoDBConnection(self.mongodb_config) as mongo:
                primary_ip = mongo.primary()
        except PyMongoError as e:
            logger.error("Unable to access primary due to: %s", e)
            return None

        # check if current unit matches primary ip
        if primary_ip == self.unit_host(self.unit):
            return self.unit.name

        # check if peer unit matches primary ip
        for unit in self.peers_units:
            if primary_ip == self.unit_host(unit):
                return unit.name

        return None

    @property
    def get_charm_internal_revision(self) -> str:
        """Returns the contents of the get_charm_internal_revision file."""
        with open(Config.CHARM_INTERNAL_VERSION_FILE, "r") as f:
            return f.read().strip()

    # END: properties

    # BEGIN: generic helper methods
    def remote_mongos_config(self, hosts) -> MongoConfiguration:
        """Generates a MongoConfiguration object for mongos in the deployment of MongoDB."""
        # mongos that are part of the cluster have the same username and password, but different
        # hosts
        return self._get_mongos_config_for_user(OperatorUser, hosts)

    def remote_mongodb_config(self, hosts, replset=None, standalone=None) -> MongoConfiguration:
        """Generates a MongoConfiguration object for mongod in the deployment of MongoDB."""
        # mongos that are part of the cluster have the same username and password, but different
        # hosts
        return self._get_mongodb_config_for_user(
            OperatorUser, hosts, replset=replset, standalone=standalone
        )

    def _scope_opj(self, scope: Scopes):
        if scope == APP_SCOPE:
            return self.app
        if scope == UNIT_SCOPE:
            return self.unit

    def _peer_data(self, scope: Scopes):
        return self.relation.data[self._scope_opj(scope)]

    @staticmethod
    def _compare_secret_ids(secret_id1: str, secret_id2: str) -> bool:
        """Reliable comparison on secret equality.

        NOTE: Secret IDs may be of any of these forms:
         - secret://9663a790-7828-4186-8b21-2624c58b6cfe/citb87nubg2s766pab40
         - secret:citb87nubg2s766pab40
        """
        if not secret_id1 or not secret_id2:
            return False

        regex = re.compile(".*[^/][/:]")

        pure_id1 = regex.sub("", secret_id1)
        pure_id2 = regex.sub("", secret_id2)

        if pure_id1 and pure_id2:
            return pure_id1 == pure_id2
        return False

    def __filesystem_handler(self, container: Container) -> bool:
        """Pushes files on the container and handle permissions."""
        try:
            # mongod needs keyFile and TLS certificates on filesystem
            self.push_tls_certificate_to_workload()
            self._push_keyfile_to_workload(container)
            self._pull_licenses(container)
            self._set_data_dir_permissions(container)

        except (PathError, ProtocolError, MissingSecretError) as e:
            logger.error("Cannot initialize workload: %r", e)
            return False

        return True

    def __configure_layers(self, container: Container) -> bool:
        """Configure the layers of the container."""
        modified = False
        current_layers = container.get_plan()
        new_layers = {
            Config.SERVICE_NAME: self._mongod_layer,
            "log_rotate": self._log_rotate_layer,
        }
        if self.is_role(Config.Role.CONFIG_SERVER):
            new_layers["mongos"] = self._mongos_layer

        # Add Pebble config layers missing or modified
        for layer_name, definition in new_layers.items():
            for service_name, service in definition.services.items():
                if current_layers.services.get(service_name) != service:
                    modified = True
                    logger.debug(f"Adding layer {service_name}.")
                    container.add_layer(layer_name, definition, combine=True)

        # We'll always have a logrotate configuration at this point.
        container.exec(["chmod", "644", "/etc/logrotate.d/mongodb"])

        return modified

    def _configure_container(self, container: Container):
        """Configure MongoDB pebble layer specification."""
        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            raise ContainerNotReadyError

        # We need to check that the storages are attached before starting the services.
        # pebble-ready is not guaranteed to run after storage-attached so this check allows
        # to ensure that the storages are attached before the pebble-ready hook is run.
        if any(not storage for storage in self.model.storages.values()):
            logger.debug("Storages are not attached yet")
            raise ContainerNotReadyError

        if not self.__filesystem_handler(container):
            raise ContainerNotReadyError

        if self.__configure_layers(container):
            container.replan()

        # when a network cuts and the pod restarts - reconnect to the exporter
        try:
            self._connect_mongodb_exporter()
            self._connect_pbm_agent()
        except MissingSecretError as e:
            logger.error("Cannot connect mongodb exporter: %r", e)
            raise ContainerNotReadyError

    # BEGIN: charm events
    def _on_upgrade(self, event: UpgradeCharmEvent):
        """Upgrade event handler.

        During an upgrade event, it will set the version in all relations,
        replan the container and process the upgrade statuses. If the upgrade
        is compatible, it will end up emitting a post upgrade event that
        verifies the health of the cluster.
        """
        if self.unit.is_leader():
            self.version_checker.set_version_across_all_relations()

        container = self.unit.get_container(Config.CONTAINER_NAME)

        # Just run the configure layers steps on the container and defer if it fails.
        try:
            self._configure_container(container)
        except ContainerNotReadyError:
            event.defer()
            return

        self.status.set_and_share_status(ActiveStatus())
        self.upgrade._reconcile_upgrade(event)
        if self.upgrade._upgrade.is_compatible:
            # Post upgrade event verifies the success of the upgrade.
            self.upgrade.post_app_upgrade_event.emit()

    def _on_mongod_pebble_ready(self, event) -> None:
        """Configure MongoDB pebble layer specification."""
        container = self.unit.get_container(Config.CONTAINER_NAME)

        # Just run the configure layers steps on the container and defer if it fails.
        try:
            self._configure_container(container)
        except ContainerNotReadyError:
            event.defer()
            return

        self.upgrade._reconcile_upgrade(event)

    def is_db_service_ready(self) -> bool:
        """Checks if the MongoDB service is ready to accept connections."""
        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            return direct_mongo.is_ready

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        """Listen to changes in application configuration.

        To prevent a user from migrating a cluster, and causing the component to become
        unresponsive therefore causing a cluster failure, error the component. This prevents it
        from executing other hooks with a new role.
        """
        # TODO in the future support migration of components
        if not self.is_role_changed():
            return

        if self.upgrade_in_progress:
            logger.warning(
                "Changing config options is not permitted during an upgrade. The charm may be in a broken, unrecoverable state."
            )
            event.defer()
            return

        logger.error(
            f"cluster migration currently not supported, cannot change from {self.model.config['role']} to {self.role}"
        )
        raise ShardingMigrationError(
            f"Migration of sharding components not permitted, revert config role to {self.role}"
        )

    def __start_checks(self) -> bool:
        """Runs the checks that are mandatory before trying to create anything mongodb related."""
        container = self.unit.get_container(Config.CONTAINER_NAME)

        try:
            self._configure_container(container)
        except ContainerNotReadyError:
            return

        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            return False

        if not container.exists(Config.SOCKET_PATH):
            logger.debug("The mongod socket is not ready yet.")
            return False

        if not self.is_db_service_ready():
            logger.debug("mongodb service is not ready yet.")
            return False

        return True

    def _on_start(self, event: StartEvent) -> None:
        """Initialise MongoDB.

        Initialisation of replSet should be made once after start.
        MongoDB needs some time to become fully started.
        This event handler is deferred if initialisation of MongoDB
        replica set fails.
        By doing so, it is guaranteed that another
        attempt at initialisation will be made.

        Initial operator user can be created only through localhost connection.
        see https://www.mongodb.com/docs/manual/core/localhost-exception/
        unfortunately, pymongo unable to create a connection that is considered
        as local connection by MongoDB, even if a socket connection is used.
        As a result, there are only hackish ways to create initial user.
        It is needed to install mongodb-clients inside the charm container
        to make this function work correctly.
        """
        if not self.__start_checks():
            event.defer()
            return

        try:
            self._connect_mongodb_exporter()
        except ChangeError as e:
            logger.error(
                "An exception occurred when starting mongodb exporter, error: %s.", str(e)
            )
            self.status.set_and_share_status(BlockedStatus("couldn't start mongodb exporter"))
            return

        # mongod is now active
        self.status.set_and_share_status(ActiveStatus())
        self.upgrade._reconcile_upgrade(event)

        if not self.unit.is_leader():
            return

        self._initialise_replica_set(event)
        try:
            self._initialise_users(event)
            self.db_initialised = True
        except RetryError:
            logger.error("Failed to initialise users. Deferring start event.")
            event.defer()
            return

    def _relation_changes_handler(self, event: RelationEvent) -> None:
        """Handles different relation events and updates MongoDB replica set."""
        self.upgrade._reconcile_upgrade(event)
        self._connect_mongodb_exporter()
        self._connect_pbm_agent()

        if isinstance(event, RelationDepartedEvent):
            if event.departing_unit.name == self.unit.name:
                self.unit_departed = True

        if not self.unit.is_leader():
            return

        if self.upgrade_in_progress:
            logger.warning(
                "Adding replicas during an upgrade is not supported. The charm may be in a broken, unrecoverable state"
            )
            event.defer()
            return

        # Admin password and keyFile should be created before running MongoDB.
        # This code runs on leader_elected event before mongod_pebble_ready
        self._generate_secrets()

        if not self.db_initialised:
            return

        self._reconcile_mongo_hosts_and_users(event)

    def _reconcile_mongo_hosts_and_users(self, event: RelationEvent) -> None:
        """Auxiliary function to reconcile mongo data for relation events.

        Args:
            event: The relation event
        """
        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members()
                mongodb_hosts = self.mongodb_config.hosts

                # compare sets of mongod replica set members and juju hosts
                # to avoid unnecessary reconfiguration.
                if replset_members == mongodb_hosts:
                    self._set_leader_unit_active_if_needed()
                    return

                logger.info("Reconfigure replica set")

                # remove members first, it is faster
                self._remove_units_from_replica_set(event, mongo, replset_members - mongodb_hosts)

                # to avoid potential race conditions -
                # remove unit before adding new replica set members
                if isinstance(event, RelationDepartedEvent) and event.unit:
                    mongodb_hosts = mongodb_hosts - set([self.unit_host(event.unit)])

                self._add_units_from_replica_set(event, mongo, mongodb_hosts - replset_members)

                # app relations should be made aware of the new set of hosts
                if not self.is_role(Config.Role.SHARD):
                    self.client_relations.update_app_relation_data()

            except NotReadyError:
                self.status.set_and_share_status(
                    WaitingStatus("waiting to reconfigure replica set")
                )
                logger.info("Deferring reconfigure: another member doing sync right now")
                event.defer()
            except PyMongoError as e:
                self.status.set_and_share_status(
                    WaitingStatus("waiting to reconfigure replica set")
                )
                logger.info("Deferring reconfigure: error=%r", e)
                event.defer()

    def _on_stop(self, event) -> None:
        if self.unit_departed:
            logger.debug(f"{self.unit.name} blocking on_stop")
            is_in_replica_set = True
            timeout = UNIT_REMOVAL_TIMEOUT
            while is_in_replica_set and timeout > 0:
                is_in_replica_set = self.is_unit_in_replica_set()
                time.sleep(1)
                timeout -= 1
                if timeout < 0:
                    raise Exception(f"{self.unit.name}.on_stop timeout exceeded")
            logger.debug("{self.unit.name} releasing on_stop")
            self.unit_departed = False

        current_unit_number = unit_number(self.unit)
        # Raise partition to prevent other units from restarting if an upgrade is in progress.
        # If an upgrade is not in progress, the leader unit will reset the partition to 0.
        if k8s_upgrade.partition.get(app_name=self.app.name) < current_unit_number:
            k8s_upgrade.partition.set(app_name=self.app.name, value=current_unit_number)
            logger.debug(f"Partition set to {current_unit_number} during stop event")
        if not self.upgrade._upgrade:
            logger.debug("Peer relation missing during stop event")
            return

        self.upgrade._upgrade.unit_state = UnitState.RESTARTING

        # According to the MongoDB documentation, before upgrading the primary, we must ensure a
        # safe primary re-election.
        try:
            if self.unit.name == self.primary:
                logger.debug("Stepping down current primary, before upgrading service...")
                self.upgrade.step_down_primary_and_wait_reelection()
        except FailedToElectNewPrimaryError:
            logger.error("Failed to reelect primary before upgrading unit.")
            return

    def _on_update_status(self, event: UpdateStatusEvent):
        # user-made mistakes might result in other incorrect statues. Prioritise informing users of
        # their mistake.
        if invalid_integration_status := self.status.get_invalid_integration_status():
            self.status.set_and_share_status(invalid_integration_status)
            return

        # no need to report on replica set status until initialised
        if not self.db_initialised:
            return

        self.upgrade._reconcile_upgrade(event)

        # Cannot check more advanced MongoDB statuses if mongod hasn't started.
        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            if not direct_mongo.is_ready:
                # edge case: mongod will fail to run if 1. they are running as shard and 2. they
                # have already been added to the cluster with internal membership via TLS and 3.
                # they remove support for TLS
                if self.is_role(Config.Role.SHARD) and self.shard.is_shard_tls_missing():
                    self.status.set_and_share_status(
                        BlockedStatus("Shard requires TLS to be enabled.")
                    )
                    return
                else:
                    self.status.set_and_share_status(WaitingStatus("Waiting for MongoDB to start"))
                    return

        # leader should periodically handle configuring the replica set. Incidents such as network
        # cuts can lead to new IP addresses and therefore will require a reconfigure. Especially
        # in the case that the leader a change in IP address it will not receive a relation event.
        if self.unit.is_leader():
            self._relation_changes_handler(event)

        self.status.set_and_share_status(self.status.process_statuses())

    # END: charm events

    # BEGIN: actions
    def _on_get_password(self, event: ActionEvent) -> None:
        """Returns the password for the user as an action response."""
        username = self._get_user_or_fail_event(
            event, default_username=OperatorUser.get_username()
        )
        if not username:
            return
        key_name = MongoDBUser.get_password_key_name_for_user(username)
        event.set_results(
            {Config.Actions.PASSWORD_PARAM_NAME: self.get_secret(APP_SCOPE, key_name)}
        )

    def _on_set_password(self, event: ActionEvent) -> None:
        """Set the password for the specified user."""
        if not self.pass_pre_set_password_checks(event):
            return

        username = self._get_user_or_fail_event(
            event, default_username=OperatorUser.get_username()
        )
        if not username:
            return

        new_password = event.params.get(Config.Actions.PASSWORD_PARAM_NAME, generate_password())

        if len(new_password) > Config.Secrets.MAX_PASSWORD_LENGTH:
            event.fail(
                f"Password cannot be longer than {Config.Secrets.MAX_PASSWORD_LENGTH} characters."
            )
            return

        try:
            secret_id = self.set_password(username, new_password)
        except SetPasswordError as e:
            event.fail(f"{e}")
            return

        if username == BackupUser.get_username():
            self._connect_pbm_agent()

        if username == MonitorUser.get_username():
            self._connect_mongodb_exporter()

        if username in [OperatorUser.get_username(), BackupUser.get_username()]:
            self.config_server.update_credentials(
                MongoDBUser.get_password_key_name_for_user(username),
                new_password,
            )

        event.set_results(
            {Config.Actions.PASSWORD_PARAM_NAME: new_password, "secret-id": secret_id}
        )

    def set_password(self, username, password) -> int:
        """Sets the password for a given username and return the secret id.

        Raises:
            SetPasswordError
        """
        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                mongo.set_user_password(username, password)
            except NotReadyError:
                raise SetPasswordError(
                    "Failed changing the password: Not all members healthy or finished initial sync."
                )
            except PyMongoError as e:
                raise SetPasswordError(f"Failed changing the password: {e}")

        return self.set_secret(
            APP_SCOPE, MongoDBUser.get_password_key_name_for_user(username), password
        )

    def _on_secret_remove(self, event):
        logging.debug(f"Secret {event.secret.id} seems to have no observers, could be removed")

    def _on_secret_changed(self, event):
        """Handles secrets changes event.

        When user run set-password action, juju leader changes the password inside the database
        and inside the secret object. This action runs the restart for monitoring tool and
        for backup tool on non-leader units to keep them working with MongoDB. The same workflow
        occurs on TLS certs change.
        """
        label = None
        if generate_secret_label(self, Config.APP_SCOPE) == event.secret.label:
            label = generate_secret_label(self, Config.APP_SCOPE)
            scope = APP_SCOPE
        elif generate_secret_label(self, Config.UNIT_SCOPE) == event.secret.label:
            label = generate_secret_label(self, Config.UNIT_SCOPE)
            scope = UNIT_SCOPE
        else:
            logging.debug("Secret %s changed, but it's unknown", event.secret.id)
            return
        logging.debug("Secret %s for scope %s changed, refreshing", event.secret.id, scope)

        self.secrets.get(label)
        self._connect_mongodb_exporter()
        self._connect_pbm_agent()

    # END: actions

    # BEGIN: user management
    @retry(
        stop=stop_after_attempt(USER_CREATING_MAX_ATTEMPTS),
        wait=wait_fixed(USER_CREATION_COOLDOWN),
        before=before_log(logger, logging.DEBUG),
    )
    def _initialise_users(self, event: StartEvent) -> None:
        """Create users.

        User creation can only be completed after the replica set has
        been initialised which requires some time.
        In race conditions this can lead to failure to initialise users.
        To prevent these race conditions from breaking the code, retry on failure.
        """
        if self.users_initialized:
            return

        # only leader should create users
        if not self.unit.is_leader():
            return

        logger.info("User initialization")
        try:
            logger.info("User initialization")
            self._init_operator_user()
            self._init_backup_user()
            self._init_monitor_user()
            # Bare replicas can create users or config-servers for related mongos apps
            if not self.is_role(Config.Role.SHARD):
                logger.info("Manage users")
                self.client_relations.oversee_users(None, event)
            self.users_initialized = True
        except ExecError as e:
            logger.error("Deferring on_start: exit code: %i, stderr: %s", e.exit_code, e.stderr)
            raise  # we need to raise to make retry work
        except PyMongoError as e:
            logger.error("Deferring on_start since: error=%r", e)
            raise  # we need to raise to make retry work
        except AdminUserCreationError:
            logger.error("Deferring on_start: Failed to create operator user.")
            raise  # we need to raise to make retry work

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_operator_user(self) -> None:
        """Creates initial operator user for MongoDB.

        Initial operator user can be created only through localhost connection.
        see https://www.mongodb.com/docs/manual/core/localhost-exception/
        unfortunately, pymongo unable to create a connection that is considered
        as local connection by MongoDB, even if a socket connection is used.
        As a result, there are only hackish ways to create initial user.
        It is needed to install mongodb-clients inside the charm container
        to make this function work correctly.
        """
        if self._is_user_created(OperatorUser) or not self.unit.is_leader():
            return

        container = self.unit.get_container(Config.CONTAINER_NAME)

        mongo_cmd = (
            "/usr/bin/mongosh" if container.exists("/usr/bin/mongosh") else "/usr/bin/mongo"
        )

        process = container.exec(
            command=get_create_user_cmd(self.mongodb_config, mongo_cmd),
            stdin=self.mongodb_config.password,
        )
        try:
            process.wait_output()
        except Exception as e:
            logger.exception("Failed to create the operator user: %s", e)
            raise AdminUserCreationError

        logger.debug(f"{OperatorUser.get_username()} user created")
        self._set_user_created(OperatorUser)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_monitor_user(self):
        """Creates the monitor user on the MongoDB database."""
        if self._is_user_created(MonitorUser):
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            logger.debug("creating the monitor user roles...")
            mongo.create_role(
                role_name=MonitorUser.get_mongodb_role(), privileges=MonitorUser.get_privileges()
            )
            logger.debug("creating the monitor user...")
            mongo.create_user(self.monitor_config)
            self._set_user_created(MonitorUser)

        self._connect_mongodb_exporter()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_backup_user(self):
        """Creates the backup user on the MongoDB database."""
        if self._is_user_created(BackupUser):
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            # first we must create the necessary roles for the PBM tool
            logger.info("creating the backup user roles...")
            mongo.create_role(
                role_name=BackupUser.get_mongodb_role(), privileges=BackupUser.get_privileges()
            )

            self._check_or_set_user_password(BackupUser)
            mongo.create_user(self.backup_config)
            self._set_user_created(BackupUser)

    # END: user management

    # BEGIN: helper functions

    def _is_user_created(self, user: MongoDBUser) -> bool:
        return f"{user.get_username()}-user-created" in self.app_peer_data

    def _set_user_created(self, user: MongoDBUser) -> None:
        self.app_peer_data[f"{user.get_username()}-user-created"] = "True"

    def _get_mongodb_config_for_user(
        self,
        user: MongoDBUser,
        hosts: List[str],
        replset: str | None = None,
        standalone: bool = False,
    ) -> MongoConfiguration:
        external_ca, _ = self.tls.get_tls_files(internal=False)
        internal_ca, _ = self.tls.get_tls_files(internal=True)
        password = self.get_secret(APP_SCOPE, user.get_password_key_name())
        if not password:
            raise MissingSecretError(
                f"Password for '{APP_SCOPE}', '{user.get_username()}' couldn't be retrieved"
            )
        else:
            return MongoConfiguration(
                replset=replset or self.app.name,
                database=user.get_database_name(),
                username=user.get_username(),
                password=password,  # type: ignore
                hosts=set(hosts),
                roles=set(user.get_roles()),
                tls_external=external_ca is not None,
                tls_internal=internal_ca is not None,
                standalone=standalone,
            )

    def _get_mongos_config_for_user(
        self, user: MongoDBUser, hosts: Set[str]
    ) -> MongoConfiguration:
        external_ca, _ = self.tls.get_tls_files(internal=False)
        internal_ca, _ = self.tls.get_tls_files(internal=True)

        return MongoConfiguration(
            database=user.get_database_name(),
            username=user.get_username(),
            password=self.get_secret(APP_SCOPE, user.get_password_key_name()),
            hosts=hosts,
            port=Config.MONGOS_PORT,
            roles=user.get_roles(),
            tls_external=external_ca is not None,
            tls_internal=internal_ca is not None,
        )

    def _get_user_or_fail_event(self, event: ActionEvent, default_username: str) -> Optional[str]:
        """Returns MongoDBUser object or raises ActionFail if user doesn't exist."""
        username = event.params.get(Config.Actions.USERNAME_PARAM_NAME, default_username)
        if username not in CHARM_USERS:
            event.fail(
                f"The action can be run only for users used by the charm:"
                f" {', '.join(CHARM_USERS)} not {username}"
            )
            return
        return username

    def pass_pre_set_password_checks(self, event: ActionEvent) -> bool:
        """Checks conditions for setting the password and fail if necessary."""
        if self.is_role(Config.Role.SHARD):
            event.fail("Cannot set password on shard, please set password on config-server.")
            return False

        # changing the backup password while a backup/restore is in progress can be disastrous
        pbm_status = self.backups.get_pbm_status()
        if isinstance(pbm_status, MaintenanceStatus):
            event.fail("Cannot change password while a backup/restore is in progress.")
            return False

        # only leader can write the new password into peer relation.
        if not self.unit.is_leader():
            event.fail("The action can be run only on leader unit.")
            return False

        if self.upgrade_in_progress:
            logger.debug("Do not set the password while a backup/restore is in progress.")
            event.fail("Cannot set passwords while an upgrade is in progress.")
            return False

        return True

    def _check_or_set_user_password(self, user: MongoDBUser) -> None:
        key = user.get_password_key_name()
        if not self.get_secret(APP_SCOPE, key):
            self.set_secret(APP_SCOPE, key, generate_password())

    def _check_or_set_keyfile(self) -> None:
        if not self.get_secret(APP_SCOPE, "keyfile"):
            self._generate_keyfile()

    def _generate_keyfile(self) -> None:
        self.set_secret(APP_SCOPE, "keyfile", generate_keyfile())

    def get_keyfile_contents(self) -> str:
        """Retrieves the contents of the keyfile on host machine."""
        # wait for keyFile to be created by leader unit
        if not self.get_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME):
            logger.debug("waiting for leader unit to generate keyfile contents")

        try:
            container = self.unit.get_container(Config.CONTAINER_NAME)
            key = container.pull(f"{Config.MONGOD_CONF_DIR}/{Config.TLS.KEY_FILE_NAME}")
            return key.read()
        except PathError:
            logger.info("no keyfile present")
            return

    def _generate_secrets(self) -> None:
        """Generate passwords and put them into peer relation.

        The same keyFile and operator password on all members are needed.
        It means it is needed to generate them once and share between members.
        NB: only leader should execute this function.
        """
        self._check_or_set_user_password(OperatorUser)
        self._check_or_set_user_password(MonitorUser)

        self._check_or_set_keyfile()

    def _initialise_replica_set(self, event: StartEvent) -> None:
        """Initialise replica set and create users."""
        if self.replica_set_initialised:
            # The replica set should be initialised only once. Check should be
            # external (e.g., check initialisation inside peer relation). We
            # shouldn't rely on MongoDB response because the data directory
            # can be corrupted.
            return

        # only leader should initialise the replica set
        if not self.unit.is_leader():
            return

        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            try:
                logger.info("Replica Set initialization")
                direct_mongo.init_replset()
            except ExecError as e:
                logger.error(
                    "Deferring on_start: exit code: %i, stderr: %s", e.exit_code, e.stderr
                )
                event.defer()
                return
            except PyMongoError as e:
                logger.error("Deferring on_start since: error=%r", e)
                event.defer()
                return
            except AdminUserCreationError as e:
                logger.error("Deferring on_start because of users creation: error=%r", e)
                event.defer()
                return

        self.replica_set_initialised = True

    def _add_units_from_replica_set(
        self, event, mongo: MongoDBConnection, units_to_add: Set[str]
    ) -> None:
        for member in units_to_add:
            logger.debug("Adding %s to the replica set", member)
            with MongoDBConnection(self.mongodb_config, member, direct=True) as direct_mongo:
                if not direct_mongo.is_ready:
                    logger.debug("Deferring reconfigure: %s is not ready yet.", member)
                    event.defer()
                    return
            mongo.add_replset_member(member)

    def _remove_units_from_replica_set(
        self, evemt, mongo: MongoDBConnection, units_to_remove: Set[str]
    ) -> None:
        for member in units_to_remove:
            logger.debug("Removing %s from the replica set", member)
            mongo.remove_replset_member(member)

    def _set_leader_unit_active_if_needed(self):
        # This can happen after restart mongod when enable \ disable TLS
        if (
            isinstance(self.unit.status, WaitingStatus)
            and self.unit.status.message == "waiting to reconfigure replica set"
        ):
            self.status.set_and_share_status(ActiveStatus())

    def get_secret(self, scope: Scopes, key: str) -> Optional[str]:
        """Getting a secret."""
        label = generate_secret_label(self, scope)
        secret = self.secrets.get(label)
        if not secret:
            return

        value = secret.get_content().get(key)
        if value != Config.Secrets.SECRET_DELETED_LABEL:
            return value

    def set_secret(self, scope: Scopes, key: str, value: Optional[str]) -> Optional[str]:
        """(Re)defining a secret."""
        if not value:
            return self.remove_secret(scope, key)

        label = generate_secret_label(self, scope)
        secret = self.secrets.get(label)
        if not secret:
            self.secrets.add(label, {key: value}, scope)
        else:
            content = secret.get_content()
            content.update({key: value})
            secret.set_content(content)
        return label

    def remove_secret(self, scope, key) -> None:
        """Removing a secret."""
        label = generate_secret_label(self, scope)
        secret = self.secrets.get(label)
        if not secret:
            return

        content = secret.get_content()

        if not content.get(key) or content[key] == Config.Secrets.SECRET_DELETED_LABEL:
            logger.error(f"Non-existing secret {scope}:{key} was attempted to be removed.")
            return

        content[key] = Config.Secrets.SECRET_DELETED_LABEL
        secret.set_content(content)

    def restart_charm_services(self):
        """Restart mongod service."""
        container = self.unit.get_container(Config.CONTAINER_NAME)
        container.stop(Config.SERVICE_NAME)

        container.add_layer(Config.SERVICE_NAME, self._mongod_layer, combine=True)
        if self.is_role(Config.Role.CONFIG_SERVER):
            container.add_layer("mongos", self._mongos_layer, combine=True)

        container.replan()

        self._connect_mongodb_exporter()
        self._connect_pbm_agent()

    def _push_keyfile_to_workload(self, container: Container) -> None:
        """Upload the keyFile to a workload container."""
        keyfile = self.get_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME)
        if not keyfile:
            raise MissingSecretError(f"No secret defined for {APP_SCOPE}, keyfile")
        else:
            self.push_file_to_unit(
                container=container,
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=Config.TLS.KEY_FILE_NAME,
                file_contents=keyfile,
            )

    def push_tls_certificate_to_workload(self) -> None:
        """Uploads certificate to the workload container."""
        container = self.unit.get_container(Config.CONTAINER_NAME)

        # Handling of external CA and PEM files
        external_ca, external_pem = self.tls.get_tls_files(internal=False)

        if external_ca is not None:
            logger.debug("Uploading external ca to workload container")
            self.push_file_to_unit(
                container=container,
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=Config.TLS.EXT_CA_FILE,
                file_contents=external_ca,
            )
        if external_pem is not None:
            logger.debug("Uploading external pem to workload container")
            self.push_file_to_unit(
                container=container,
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=Config.TLS.EXT_PEM_FILE,
                file_contents=external_pem,
            )

        # Handling of external CA and PEM files
        internal_ca, internal_pem = self.tls.get_tls_files(internal=True)

        if internal_ca is not None:
            logger.debug("Uploading internal ca to workload container")
            self.push_file_to_unit(
                container=container,
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=Config.TLS.INT_CA_FILE,
                file_contents=internal_ca,
            )
        if internal_pem is not None:
            logger.debug("Uploading internal pem to workload container")
            self.push_file_to_unit(
                container=container,
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=Config.TLS.INT_PEM_FILE,
                file_contents=internal_pem,
            )

    def push_file_to_unit(
        self, parent_dir: str, file_name: str, file_contents: str, container: Container = None
    ) -> None:
        """Push the file on the container, with the right permissions."""
        container = container or self.unit.get_container(Config.CONTAINER_NAME)
        container.push(
            f"{parent_dir}/{file_name}",
            file_contents,
            make_dirs=True,
            permissions=0o400,
            user=Config.UNIX_USER,
            group=Config.UNIX_GROUP,
        )

    def delete_tls_certificate_from_workload(self) -> None:
        """Deletes certificate from the workload container."""
        logger.info("Deleting TLS certificate from workload container")
        container = self.unit.get_container(Config.CONTAINER_NAME)
        for file in [
            Config.TLS.EXT_CA_FILE,
            Config.TLS.EXT_PEM_FILE,
            Config.TLS.INT_CA_FILE,
            Config.TLS.INT_PEM_FILE,
        ]:
            try:
                container.remove_path(f"{Config.MONGOD_CONF_DIR}/{file}")
            except PathError as err:
                logger.debug("Path unavailable: %s (%s)", file, str(err))

    def unit_host(self, unit: Unit) -> str:
        """Create a DNS name for a MongoDB unit.

        Args:
            unit_name: the juju unit name, e.g. "mongodb/1".

        Returns:
            A string representing the hostname of the MongoDB unit.
        """
        unit_id = unit.name.split("/")[1]
        return f"{self.app.name}-{unit_id}.{self.app.name}-endpoints"

    def _connect_mongodb_exporter(self) -> None:
        """Exposes the endpoint to mongodb_exporter."""
        container = self.unit.get_container(Config.CONTAINER_NAME)

        if not container.can_connect():
            return

        if not self.db_initialised:
            return

        # must wait for leader to set URI before connecting
        if not self.get_secret(APP_SCOPE, MonitorUser.get_password_key_name()):
            return

        current_service_config = (
            container.get_plan().to_dict().get("services", {}).get("mongodb_exporter", {})
        )
        new_service_config = self._monitor_layer.services.get("mongodb_exporter", {})

        if current_service_config == new_service_config:
            return

        # Add initial Pebble config layer using the Pebble API
        # mongodb_exporter --mongodb.uri=

        container.add_layer("mongodb_exporter", self._monitor_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()

    def _connect_pbm_agent(self) -> None:
        """Updates URI for pbm-agent."""
        container = self.unit.get_container(Config.CONTAINER_NAME)

        if not container.can_connect():
            logger.debug(
                "_connect_pbm_agent: can't connect to container %s. Returning.", container
            )
            return

        if not self.db_initialised:
            return

        # pbm is not functional in shards without a config-server
        if self.is_role(Config.Role.SHARD) and not (
            self.has_config_server() and self.shard._is_added_to_cluster()
        ):
            logger.debug("Cannot start pbm on shard until shard is added to cluster.")
            return

        # must wait for leader to set URI before any attempts to update are made
        if not self.get_secret(APP_SCOPE, BackupUser.get_password_key_name()):
            logger.debug("_connect_pbm_agent: can't get backup user password!. Returning.")
            return

        current_service_config = (
            container.get_plan().to_dict().get("services", {}).get(Config.Backup.SERVICE_NAME, {})
        )
        new_service_config = self._backup_layer.services.get(Config.Backup.SERVICE_NAME, {})

        if current_service_config == new_service_config:
            return

        container.add_layer(Config.Backup.SERVICE_NAME, self._backup_layer, combine=True)
        for attempt in Retrying(
            stop=stop_after_attempt(10),
            wait=wait_fixed(5),
            reraise=True,
        ):
            with attempt:
                container.replan()

    def has_backup_service(self) -> bool:
        """Returns the backup service."""
        container = self.unit.get_container(Config.CONTAINER_NAME)
        try:
            container.get_service(Config.Backup.SERVICE_NAME)
            return True
        except ModelError:
            return False

    def is_unit_in_replica_set(self) -> bool:
        """Check if the unit is in the replica set."""
        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members()
                return self.unit_host(self.unit) in replset_members
            except NotReadyError as e:
                logger.error(f"{self.unit.name}.is_unit_in_replica_set NotReadyError={e}")
            except PyMongoError as e:
                logger.error(f"{self.unit.name}.is_unit_in_replica_set PyMongoError={e}")
        return False

    def run_pbm_command(self, cmd: List[str]) -> str:
        """Executes a command in the workload container."""
        container = self.unit.get_container(Config.CONTAINER_NAME)
        environment = {"PBM_MONGODB_URI": f"{self.backup_config.uri}"}
        process = container.exec([Config.Backup.PBM_PATH] + cmd, environment=environment)
        stdout, _ = process.wait_output()
        return stdout

    def clear_pbm_config_file(self) -> None:
        """Overwrites existing config file with an empty file."""
        container = self.unit.get_container(Config.CONTAINER_NAME)
        container.push(
            Config.Backup.PBM_CONFIG_FILE_PATH,
            "# this file is to be left empty. Changes in this file will be ignored.\n",
            make_dirs=True,
            permissions=0o400,
        )
        try:
            self.run_pbm_command(
                [
                    "config",
                    "--file",
                    Config.Backup.PBM_CONFIG_FILE_PATH,
                ]
            )
        except ExecError as e:
            logger.error(f"Failed to set pbm config file. {e}")
            self.unit.status = BlockedStatus(self.backups.process_pbm_error(e.stdout))
        return

    def start_backup_service(self) -> None:
        """Starts the backup service."""
        container = self.unit.get_container(Config.CONTAINER_NAME)
        container.start(Config.Backup.SERVICE_NAME)

    def restart_backup_service(self) -> None:
        """Restarts the backup service."""
        container = self.unit.get_container(Config.CONTAINER_NAME)
        container.restart(Config.Backup.SERVICE_NAME)

    def check_relation_broken_or_scale_down(self, event: RelationDepartedEvent) -> None:
        """Checks relation departed event is the result of removed relation or scale down.

        Relation departed and relation broken events occur during scaling down or during relation
        removal, only relation departed events have access to metadata to determine which case.
        """
        scaling_down = self.set_scaling_down(event)

        if scaling_down:
            logger.info(
                "Scaling down the application, no need to process removed relation in broken hook."
            )

    def is_scaling_down(self, rel_id: int) -> bool:
        """Returns True if the application is scaling down."""
        rel_departed_key = self._generate_relation_departed_key(rel_id)
        return json.loads(self.unit_peer_data[rel_departed_key])

    def has_departed_run(self, rel_id: int) -> bool:
        """Returns True if the relation departed event has run."""
        rel_departed_key = self._generate_relation_departed_key(rel_id)
        return rel_departed_key in self.unit_peer_data

    def set_scaling_down(self, event: RelationDepartedEvent) -> bool:
        """Sets whether or not the current unit is scaling down."""
        # check if relation departed is due to current unit being removed. (i.e. scaling down the
        # application.)
        rel_departed_key = self._generate_relation_departed_key(event.relation.id)
        scaling_down = event.departing_unit == self.unit
        self.unit_peer_data[rel_departed_key] = json.dumps(scaling_down)
        return scaling_down

    def proceed_on_broken_event(self, event) -> bool:
        """Returns relation_id if relation broken event occurred due to a removed relation."""
        # Only relation_deparated events can check if scaling down
        departed_relation_id = event.relation.id
        if not self.has_departed_run(departed_relation_id):
            logger.info(
                "Deferring, must wait for relation departed hook to decide if relation should be removed."
            )
            event.defer()
            return False

        # check if were scaling down and add a log message
        if self.is_scaling_down(departed_relation_id):
            logger.info(
                "Relation broken event occurring due to scale down, do not proceed to remove users."
            )
            return False

        return True

    @staticmethod
    def _generate_relation_departed_key(rel_id: int) -> str:
        """Generates the relation departed key for a specified relation id."""
        return f"relation_{rel_id}_departed"

    def is_relation_feasible(self, rel_interface: str) -> bool:
        """Returns true if the proposed relation is feasible.

        TODO add checks for version mismatch
        """
        if self.is_sharding_component() and rel_interface in Config.Relations.DB_RELATIONS:
            self.status.set_and_share_status(
                BlockedStatus(f"Sharding roles do not support {rel_interface} interface.")
            )
            logger.error(
                "Charm is in sharding role: %s. Does not support %s interface.",
                self.role,
                rel_interface,
            )
            return False

        if (
            not self.is_sharding_component()
            and rel_interface == Config.Relations.SHARDING_RELATIONS_NAME
        ):
            self.status.set_and_share_status(
                BlockedStatus("sharding interface cannot be used by replicas")
            )
            logger.error(
                "Charm is in sharding role: %s. Does not support %s interface.",
                self.role,
                rel_interface,
            )
            return False

        if revision_mismatch_status := self.status.get_cluster_mismatched_revision_status():
            self.status.set_and_share_status(revision_mismatch_status)
            return False

        return True

    def is_sharding_component(self) -> bool:
        """Returns true if charm is running as a sharded component."""
        return self.is_role(Config.Role.SHARD) or self.is_role(Config.Role.CONFIG_SERVER)

    def is_cluster_on_same_revision(self) -> bool:
        """Returns True if the cluster is using the same charm revision.

        Note: This can only be determined by the config-server since shards are not integrated to
        each other.
        """
        if not self.is_role(Config.Role.CONFIG_SERVER):
            raise NotConfigServerError("This check can only be ran by the config-server.")

        return self.version_checker.are_related_apps_valid()

    # END: helper functions

    # BEGIN: static methods
    @staticmethod
    def _pull_licenses(container: Container) -> None:
        """Pull licences from workload."""
        licenses = [
            "snap",
            "rock",
            "mongodb-exporter",
            "percona-backup-mongodb",
            "percona-server",
        ]

        for license_name in licenses:
            try:
                # Lazy copy, only if the file wasn't already pulled.
                filename = Path(f"LICENSE_{license_name}")
                if not filename.is_file():
                    license_file = container.pull(path=Config.get_license_path(license_name))
                    filename.write_text(str(license_file.read()))
            except FileExistsError:
                pass

    @staticmethod
    def _set_data_dir_permissions(container: Container) -> None:
        """Ensure the data directory for mongodb is writable for the "mongodb" user.

        Until the ability to set fsGroup and fsGroupChangePolicy via Pod securityContext
        is available, we fix permissions incorrectly with chown.
        """
        # Ensure the log status dir exists
        container.make_dir(Config.LogRotate.LOG_STATUS_DIR, make_parents=True)

        for path in [Config.DATA_DIR, Config.LOG_DIR, Config.LogRotate.LOG_STATUS_DIR]:
            paths = container.list_files(path, itself=True)
            assert len(paths) == 1, "list_files doesn't return only the directory itself"
            logger.debug(f"Data directory ownership: {paths[0].user}:{paths[0].group}")
            if paths[0].user != Config.UNIX_USER or paths[0].group != Config.UNIX_GROUP:
                container.exec(
                    f"chown {Config.UNIX_USER}:{Config.UNIX_GROUP} -R {path}".split(" ")
                )

    # END: static methods


class SetPasswordError(Exception):
    """Raised on failure to set password for MongoDB user."""


class ShardingMigrationError(Exception):
    """Raised when there is an attempt to change the role of a sharding component."""


if __name__ == "__main__":
    main(MongoDBCharm)
