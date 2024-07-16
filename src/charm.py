#!/usr/bin/env python3
"""Charm code for MongoDB service on Kubernetes."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import re
import time
from typing import Dict, List, Optional, Set

from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v0.loki_push_api import LogProxyConsumer
from charms.mongodb.v0.mongodb import (
    MongoDBConfiguration,
    MongoDBConnection,
    NotReadyError,
)
from charms.mongodb.v0.mongodb_secrets import SecretCache, generate_secret_label
from charms.mongodb.v0.mongodb_tls import MongoDBTLS
from charms.mongodb.v0.set_status import MongoDBStatusHandler
from charms.mongodb.v0.users import (
    CHARM_USERS,
    BackupUser,
    MongoDBUser,
    MonitorUser,
    OperatorUser,
)
from charms.mongodb.v1.helpers import (
    build_unit_status,
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    get_mongod_args,
)
from charms.mongodb.v1.mongodb_backups import S3_RELATION, MongoDBBackups
from charms.mongodb.v1.mongodb_provider import MongoDBProvider
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from ops.charm import (
    ActionEvent,
    CharmBase,
    RelationDepartedEvent,
    StartEvent,
    UpdateStatusEvent,
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

from config import Config
from exceptions import AdminUserCreationError, MissingSecretError

logger = logging.getLogger(__name__)

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
        self.framework.observe(self.on.start, self._on_start)
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

        self.client_relations = MongoDBProvider(self)
        self.tls = MongoDBTLS(self, Config.Relations.PEERS, Config.SUBSTRATE)
        self.backups = MongoDBBackups(self)

        self.metrics_endpoint = MetricsEndpointProvider(
            self, refresh_event=self.on.start, jobs=Config.Monitoring.JOBS
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

    # BEGIN: properties

    @property
    def _unit_hosts(self) -> List[str]:
        """Retrieve IP addresses associated with MongoDB application.

        Returns:
            a list of IP address associated with MongoDB application.
        """
        self_unit = [self.get_hostname_for_unit(self.unit)]

        if not self._peers:
            return self_unit

        return self_unit + [self.get_hostname_for_unit(unit) for unit in self._peers.units]

    @property
    def _peers(self) -> Optional[Relation]:
        """Fetch the peer relation.

        Returns:
             An `ops.model.Relation` object representing the peer relation.
        """
        return self.model.get_relation(Config.Relations.PEERS)

    @property
    def mongodb_config(self) -> MongoDBConfiguration:
        """Create a configuration object with settings.

        Needed for correct handling interactions with MongoDB.

        Returns:
            A MongoDBConfiguration object
        """
        return self._get_mongodb_config_for_user(OperatorUser, self._unit_hosts)

    @property
    def monitor_config(self) -> MongoDBConfiguration:
        """Generates a MongoDBConfiguration object for this deployment of MongoDB."""
        return self._get_mongodb_config_for_user(
            MonitorUser, [self.get_hostname_for_unit(self.unit)]
        )

    @property
    def backup_config(self) -> MongoDBConfiguration:
        """Generates a MongoDBConfiguration object for backup."""
        return self._get_mongodb_config_for_user(
            BackupUser, [self.get_hostname_for_unit(self.unit)]
        )

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
                    "command": "mongod " + get_mongod_args(self.mongodb_config),
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
        return Layer(layer_config)  # type: ignore

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
        return Layer(layer_config)  # type: ignore

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
        return "db_initialised" in self.app_peer_data

    def is_role(self, role_name: str) -> bool:
        """TODO: Implement this as part of sharding."""
        return False

    def has_config_server(self) -> bool:
        """TODO: Implement this function as part of sharding."""
        return False

    def get_config_server_name(self) -> None:
        """TODO: Implement this function as part of sharding."""
        return None

    @db_initialised.setter
    def db_initialised(self, value):
        """Set the db_initialised flag."""
        if isinstance(value, bool):
            self.app_peer_data["db_initialised"] = str(value)
        else:
            raise ValueError(
                f"'db_initialised' must be a boolean value. Proivded: {value} is of type {type(value)}"
            )

    @property
    def upgrade_in_progress(self):
        """TODO: implement this as part of upgrades."""
        return False

    @property
    def replica_set_initialised(self) -> bool:
        """Check if the MongoDB replica set is initialised."""
        return "replica_set_initialised" in self.app_peer_data

    @replica_set_initialised.setter
    def replica_set_initialised(self, value):
        """Set the replica_set_initialised flag."""
        if isinstance(value, bool):
            self.app_peer_data["replica_set_initialised"] = str(value)
        else:
            raise ValueError(
                f"'replica_set_initialised' must be a boolean value. Proivded: {value} is of type {type(value)}"
            )

    @property
    def users_initialized(self) -> bool:
        """Check if MongoDB users are created."""
        return "users_initialized" in self.app_peer_data

    @users_initialized.setter
    def users_initialized(self, value):
        """Set the users_initialized flag."""
        if isinstance(value, bool):
            self.app_peer_data["users_initialized"] = str(value)
        else:
            raise ValueError(
                f"'users_initialized' must be a boolean value. Proivded: {value} is of type {type(value)}"
            )

    # END: properties

    # BEGIN: generic helper methods

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

    # END: generic helper methods

    # BEGIN: charm events
    def _on_mongod_pebble_ready(self, event) -> None:
        """Configure MongoDB pebble layer specification."""
        # Get a reference the container attribute
        container = self.unit.get_container(Config.CONTAINER_NAME)
        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return

        try:
            # mongod needs keyFile and TLS certificates on filesystem
            self.push_tls_certificate_to_workload()
            self._push_keyfile_to_workload(container)
            self._pull_licenses(container)
            self._set_data_dir_permissions(container)

        except (PathError, ProtocolError, MissingSecretError) as e:
            logger.error("Cannot initialize workload: %r", e)
            event.defer()
            return

        # Add initial Pebble config layer using the Pebble API
        container.add_layer("mongod", self._mongod_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()

        # when a network cuts and the pod restarts - reconnect to the exporter
        try:
            self._connect_mongodb_exporter()
            self._connect_pbm_agent()
        except MissingSecretError as e:
            logger.error("Cannot connect mongodb exporter: %r", e)
            event.defer()
            return

    def is_db_service_ready(self) -> bool:
        """Checks if the MongoDB service is ready to accept connections."""
        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            return direct_mongo.is_ready

    def _on_start(self, event) -> None:
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
        container = self.unit.get_container(Config.CONTAINER_NAME)
        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return

        if not container.exists(Config.SOCKET_PATH):
            logger.debug("The mongod socket is not ready yet.")
            event.defer()
            return

        if not self.is_db_service_ready():
            logger.debug("mongodb service is not ready yet.")
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

    def _relation_changes_handler(self, event) -> None:
        """Handles different relation events and updates MongoDB replica set."""
        self._connect_mongodb_exporter()
        self._connect_pbm_agent()

        if type(event) is RelationDepartedEvent:
            if event.departing_unit.name == self.unit.name:
                self.unit_peer_data.setdefault("unit_departed", "True")

        if not self.unit.is_leader():
            return

        # Admin password and keyFile should be created before running MongoDB.
        # This code runs on leader_elected event before mongod_pebble_ready
        self._generate_secrets()

        if not self.db_initialised:
            return

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
                    mongodb_hosts = mongodb_hosts - set([self.get_hostname_for_unit(event.unit)])

                self._add_units_from_replica_set(event, mongo, mongodb_hosts - replset_members)

                # app relations should be made aware of the new set of hosts
                self.client_relations.update_app_relation_data()

            except NotReadyError:
                logger.info("Deferring reconfigure: another member doing sync right now")
                event.defer()
            except PyMongoError as e:
                logger.info("Deferring reconfigure: error=%r", e)
                event.defer()

    def _on_stop(self, event) -> None:
        if "True" == self.unit_peer_data.get("unit_departed", "False"):
            logger.debug(f"{self.unit.name} blocking on_stop")
            is_in_replica_set = True
            timeout = UNIT_REMOVAL_TIMEOUT
            while is_in_replica_set and timeout > 0:
                is_in_replica_set = self.is_unit_in_replica_set()
                time.sleep(1)
                timeout -= 1
                if timeout < 0:
                    raise Exception(f"{self.unit.name}.on_stop timeout exceeded")
            logger.debug(f"{self.unit.name} releasing on_stop")
            self.unit_peer_data["unit_departed"] = ""

    def _on_update_status(self, event: UpdateStatusEvent):
        # no need to report on replica set status until initialised
        if not self.db_initialised:
            return

        # Cannot check more advanced MongoDB statuses if mongod hasn't started.
        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            if not direct_mongo.is_ready:
                self.status.set_and_share_status(WaitingStatus("Waiting for MongoDB to start"))
                return

        # leader should periodically handle configuring the replica set. Incidents such as network
        # cuts can lead to new IP addresses and therefore will require a reconfigure. Especially
        # in the case that the leader a change in IP address it will not receive a relation event.
        if self.unit.is_leader():
            self._relation_changes_handler(event)

        # update the units status based on it's replica set config and backup status. An error in
        # the status of MongoDB takes precedence over pbm status.
        mongodb_status = build_unit_status(
            self.mongodb_config, self.get_hostname_for_unit(self.unit)
        )
        pbm_status = self.backups.get_pbm_status()
        if (
            not isinstance(mongodb_status, ActiveStatus)
            or not self.model.get_relation(
                S3_RELATION
            )  # if s3 relation doesn't exist only report MongoDB status
            or isinstance(pbm_status, ActiveStatus)  # pbm is ready then report the MongoDB status
        ):
            self.unit.status = mongodb_status
        else:
            self.unit.status = pbm_status

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
            logger.info("Reconcile relations")
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
            event.defer()
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
        if self._is_user_created(OperatorUser):
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
        self, user: MongoDBUser, hosts: List[str]
    ) -> MongoDBConfiguration:
        external_ca, _ = self.tls.get_tls_files(internal=False)
        internal_ca, _ = self.tls.get_tls_files(internal=True)
        password = self.get_secret(APP_SCOPE, user.get_password_key_name())
        if not password:
            raise MissingSecretError(
                f"Password for '{APP_SCOPE}', '{user.get_username()}' couldn't be retrieved"
            )
        else:
            return MongoDBConfiguration(
                replset=self.app.name,
                database=user.get_database_name(),
                username=user.get_username(),
                password=password,  # type: ignore
                hosts=set(hosts),
                roles=set(user.get_roles()),
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
        # changing the backup password while a backup/restore is in progress can be disastrous
        pbm_status = self.backups.get_pbm_status()
        if isinstance(pbm_status, MaintenanceStatus):
            event.fail("Cannot change password while a backup/restore is in progress.")
            return False

        # only leader can write the new password into peer relation.
        if not self.unit.is_leader():
            event.fail("The action can be run only on leader unit.")
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

        container.add_layer("mongod", self._mongod_layer, combine=True)
        container.replan()

        self._connect_mongodb_exporter()
        self._connect_pbm_agent()

    def _push_keyfile_to_workload(self, container: Container) -> None:
        """Upload the keyFile to a workload container."""
        keyfile = self.get_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME)
        if not keyfile:
            raise MissingSecretError(f"No secret defined for {APP_SCOPE}, keyfile")
        else:
            self.push_file_to_container(
                container, Config.CONF_DIR, Config.TLS.KEY_FILE_NAME, keyfile
            )

    def push_tls_certificate_to_workload(self) -> None:
        """Uploads certificate to the workload container."""
        container = self.unit.get_container(Config.CONTAINER_NAME)

        # Handling of external CA and PEM files
        external_ca, external_pem = self.tls.get_tls_files(internal=False)

        if external_ca is not None:
            logger.debug("Uploading external ca to workload container")
            self.push_file_to_container(
                container=container,
                parent_dir=Config.CONF_DIR,
                file_name=Config.TLS.EXT_CA_FILE,
                source=external_ca,
            )
        if external_pem is not None:
            logger.debug("Uploading external pem to workload container")
            self.push_file_to_container(
                container=container,
                parent_dir=Config.CONF_DIR,
                file_name=Config.TLS.EXT_PEM_FILE,
                source=external_pem,
            )

        # Handling of external CA and PEM files
        internal_ca, internal_pem = self.tls.get_tls_files(internal=True)

        if internal_ca is not None:
            logger.debug("Uploading internal ca to workload container")
            self.push_file_to_container(
                container=container,
                parent_dir=Config.CONF_DIR,
                file_name=Config.TLS.INT_CA_FILE,
                source=internal_ca,
            )
        if internal_pem is not None:
            logger.debug("Uploading internal pem to workload container")
            self.push_file_to_container(
                container=container,
                parent_dir=Config.CONF_DIR,
                file_name=Config.TLS.INT_PEM_FILE,
                source=internal_pem,
            )

    def push_file_to_container(
        self, container: Container, parent_dir: str, file_name: str, source: str
    ) -> None:
        """Push the file on the container, with the right permissions."""
        container.push(
            f"{parent_dir}/{file_name}",
            source,
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
                container.remove_path(f"{Config.CONF_DIR}/{file}")
            except PathError as err:
                logger.debug("Path unavailable: %s (%s)", file, str(err))

    def get_hostname_for_unit(self, unit: Unit) -> str:
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
                return self.get_hostname_for_unit(self.unit) in replset_members
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

    def set_scaling_down(self, event: RelationDepartedEvent) -> bool:
        """Sets whether or not the current unit is scaling down."""
        # check if relation departed is due to current unit being removed. (i.e. scaling down the
        # application.)
        rel_departed_key = self._generate_relation_departed_key(event.relation.id)
        scaling_down = event.departing_unit == self.unit
        self.unit_peer_data[rel_departed_key] = json.dumps(scaling_down)
        return scaling_down

    @staticmethod
    def _generate_relation_departed_key(rel_id: int) -> str:
        """Generates the relation departed key for a specified relation id."""
        return f"relation_{rel_id}_departed"

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
                license_file = container.pull(path=Config.get_license_path(license_name))
                f = open("LICENSE", "x")
                f.write(str(license_file.read()))
                f.close()
            except FileExistsError:
                pass

    @staticmethod
    def _set_data_dir_permissions(container: Container) -> None:
        """Ensure the data directory for mongodb is writable for the "mongodb" user.

        Until the ability to set fsGroup and fsGroupChangePolicy via Pod securityContext
        is available, we fix permissions incorrectly with chown.
        """
        for path in [Config.DATA_DIR, Config.LOG_DIR]:
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


if __name__ == "__main__":
    main(MongoDBCharm)
