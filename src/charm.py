#!/usr/bin/env python3
"""Charm code for MongoDB service on Kubernetes."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from typing import Dict, List, Optional

from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v0.loki_push_api import LogProxyConsumer
from charms.mongodb.v0.helpers import (
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    get_mongod_args,
)
from charms.mongodb.v0.mongodb import (
    MongoDBConfiguration,
    MongoDBConnection,
    NotReadyError,
)
from charms.mongodb.v0.mongodb_provider import MongoDBProvider
from charms.mongodb.v0.mongodb_tls import MongoDBTLS
from charms.mongodb.v0.users import CHARM_USERS, MongoDBUser, MonitorUser, OperatorUser
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from ops.charm import (
    ActionEvent,
    CharmBase,
    LeaderElectedEvent,
    RelationDepartedEvent,
    RelationEvent,
    RelationJoinedEvent,
    StartEvent,
    StorageDetachingEvent,
)
from ops.main import main
from ops.model import ActiveStatus, Container, Relation, Unit, WaitingStatus
from ops.pebble import ExecError, Layer, PathError, ProtocolError
from pymongo.errors import PyMongoError
from tenacity import Retrying, before_log, retry, stop_after_attempt, wait_fixed

from config import Config
from exceptions import AdminUserCreationError

logger = logging.getLogger(__name__)


class MongoDBCharm(CharmBase):
    """A Juju Charm to deploy MongoDB on Kubernetes."""

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.mongod_pebble_ready, self.on_mongod_pebble_ready)
        self.framework.observe(self.on.start, self._on_start)

        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_joined, self._on_relation_joined
        )

        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_changed, self._on_relation_handler
        )

        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_departed, self._on_relation_departed
        )

        # if a new leader has been elected update hosts of MongoDB
        self.framework.observe(self.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self.on.mongodb_storage_detaching, self._on_storage_detaching)

        self.framework.observe(self.on.get_password_action, self._on_get_password)
        self.framework.observe(self.on.set_password_action, self._on_set_password)

        self.client_relations = MongoDBProvider(self)
        self.tls = MongoDBTLS(self, Config.Relations.PEERS)

        self.metrics_endpoint = MetricsEndpointProvider(
            self, refresh_event=self.on.start, jobs=Config.Monitoring.JOBS
        )
        self.grafana_dashboards = GrafanaDashboardProvider(self)
        self.loki_push = LogProxyConsumer(
            self,
            log_files=Config.LOG_FILES,
            relation_name=Config.Relations.LOGGING,
            container_name=Config.CONTAINER_NAME,
        )

    # BEGIN: properties

    @property
    def _unit_hosts(self) -> List[str]:
        """Retrieve IP addresses associated with MongoDB application.

        Returns:
            a list of IP address associated with MongoDB application.
        """
        return [self._get_hostname_for_unit(self.unit)] + [
            self._get_hostname_for_unit(unit) for unit in self._peers.units
        ]

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
            MonitorUser, [self._get_hostname_for_unit(self.unit)]
        )

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
        return Layer(layer_config)

    @property
    def unit_peer_data(self) -> Dict:
        """Peer relation data object."""
        relation = self.model.get_relation(Config.Relations.PEERS)
        if relation is None:
            return {}

        return relation.data[self.unit]

    @property
    def app_peer_data(self) -> Dict:
        """Peer relation data object."""
        relation = self.model.get_relation(Config.Relations.PEERS)
        if relation is None:
            return {}

        return relation.data[self.app]

    @property
    def _db_initialised(self) -> bool:
        return "db_initialised" in self.app_peer_data

    @_db_initialised.setter
    def _db_initialised(self, value):
        if isinstance(value, bool):
            self.app_peer_data["db_initialised"] = str(value)
        else:
            raise ValueError(
                f"'db_initialised' must be a boolean value. Proivded: {value} is of type {type(value)}"
            )

    # END: properties

    # BEGIN: charm events
    def on_mongod_pebble_ready(self, event) -> None:
        """Configure MongoDB pebble layer specification."""
        # Get a reference the container attribute
        container = self.unit.get_container(Config.CONTAINER_NAME)
        # mongod needs keyFile and TLS certificates on filesystem

        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return
        try:
            self._push_certificate_to_workload(container)
            self._push_keyfile_to_workload(container)
            self._pull_licenses(container)
            self._fix_data_dir(container)

        except (PathError, ProtocolError) as e:
            logger.error("Cannot put keyFile: %r", e)
            event.defer()
            return

        # This function can be run in two cases:
        # 1) during regular charm start.
        # 2) if we forcefully want to apply new
        # mongod cmd line arguments (returned from get_mongod_args).
        # In the second case, we should restart mongod
        # service only if arguments changed.
        services = container.get_services(Config.SERVICE_NAME)
        if services and services[Config.SERVICE_NAME].is_running():
            new_command = "mongod " + get_mongod_args(self.mongodb_config)
            cur_command = container.get_plan().services[Config.SERVICE_NAME].command
            if new_command != cur_command:
                logger.debug("restart MongoDB due to arguments change: %s", new_command)
                container.stop(Config.SERVICE_NAME)

        # Add initial Pebble config layer using the Pebble API
        container.add_layer("mongod", self._mongod_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()

        # when a network cuts and the pod restarts - reconnect to the exporter
        self._connect_mongodb_exporter()

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

        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            if not direct_mongo.is_ready:
                logger.debug("mongodb service is not ready yet.")
                event.defer()
                return

        # mongod is now active
        self.unit.status = ActiveStatus()
        self._connect_mongodb_exporter()

        self._initialise_replica_set(event)

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        """Add peer to replica set.

        Args:
            event: The triggering relation joined event.
        """
        if not self.unit.is_leader():
            return

        self._on_relation_handler(event)

        # app relations should be made aware of the new set of hosts
        try:
            self._update_app_relation_data()
        except PyMongoError as e:
            logger.error(
                "_on_relation_joined: Deferring on updating app relation data since: error: %r", e
            )
            event.defer()
            return

    def _on_relation_handler(self, event: RelationEvent) -> None:
        """Adds the unit as a replica to the MongoDB replica set.

        Args:
            event: The triggering relation joined/changed event.
        """
        # changing the monitor password will lead to non-leader units receiving a relation changed
        # event. We must update the monitor and pbm URI if the password changes so that COS/pbm
        # can continue to work
        self._connect_mongodb_exporter()

        # only leader should configure replica set and app-changed-events can trigger the relation
        # changed hook resulting in no JUJU_REMOTE_UNIT if this is the case we should return
        # further reconfiguration can be successful only if a replica set is initialised.
        if not (self.unit.is_leader() and event.unit) or not self._db_initialised:
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members()
                # compare set of mongod replica set members and juju hosts to avoid the unnecessary
                # reconfiguration.
                if replset_members == self.mongodb_config.hosts:
                    return

                for member in self.mongodb_config.hosts - replset_members:
                    logger.debug("Adding %s to replica set", member)
                    with MongoDBConnection(
                        self.mongodb_config, member, direct=True
                    ) as direct_mongo:
                        if not direct_mongo.is_ready:
                            self.unit.status = WaitingStatus("waiting to reconfigure replica set")
                            logger.debug("Deferring reconfigure: %s is not ready yet.", member)
                            event.defer()
                            return
                    mongo.add_replset_member(member)
                    self.unit.status = ActiveStatus()
            except NotReadyError:
                self.unit.status = WaitingStatus("waiting to reconfigure replica set")
                logger.error("Deferring reconfigure: another member doing sync right now")
                event.defer()
            except PyMongoError as e:
                self.unit.status = WaitingStatus("waiting to reconfigure replica set")
                logger.error("Deferring reconfigure: error=%r", e)
                event.defer()

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Remove peer from replica set if it wasn't able to remove itself.

        Args:
            event: The triggering relation departed event.
        """
        # allow leader to update relation data and hosts if it isn't leaving
        if not self.unit.is_leader() or event.departing_unit == self.unit:
            return

        self._update_hosts(event)

        # app relations should be made aware of the new set of hosts
        try:
            self._update_app_relation_data()
        except PyMongoError as e:
            logger.error(
                "_on_relation_departed: Deferring on updating app relation data since: error: %r",
                e,
            )
            event.defer()
            return

    def _on_leader_elected(self, event: LeaderElectedEvent) -> None:
        """Handle leader elected event."""
        if not self.get_secret("app", "keyfile"):
            self._generate_secrets()

        self._update_hosts(event)

        # app relations should be made aware of the new set of hosts
        try:
            self._update_app_relation_data()
        except PyMongoError as e:
            logger.error(
                "_on_leader_elected: Deferring on updating app relation data since: error: %r", e
            )
            event.defer()
            return

    def _on_storage_detaching(self, event: StorageDetachingEvent) -> None:
        """Before storage detaches, allow removing unit to remove itself from the set.

        If the removing unit is primary also allow it to step down and elect another unit as
        primary while it still has access to its storage.
        """
        # if we are removing the last replica it will not be able to step down as primary and we
        # cannot reconfigure the replica set to have 0 members. To prevent retrying for 10 minutes
        # set this flag to True. please note that planned_units will always be >=1. When planned
        # units is 1 that means there are no other peers expected.
        single_node_replica_set = self.app.planned_units() == 1 and len(self._peers.units) == 0
        if single_node_replica_set:
            return

        try:
            # retries over a period of 10 minutes in an attempt to resolve race conditions it is
            # not possible to defer in storage detached.
            logger.debug("Removing %s from replica set", self._get_hostname_for_unit(self.unit))
            for attempt in Retrying(
                stop=stop_after_attempt(10),
                wait=wait_fixed(1),
                reraise=True,
            ):
                with attempt:
                    # remove_replset_member retries for 60 seconds
                    with MongoDBConnection(self.mongodb_config) as mongo:
                        hostname = self._get_hostname_for_unit(self.unit)
                        mongo.remove_replset_member(hostname)
        except NotReadyError:
            logger.info(
                "Failed to remove %s from replica set, another member is syncing", self.unit.name
            )
        except PyMongoError as e:
            logger.error("Failed to remove %s from replica set, error=%r", self.unit.name, e)

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
        event.set_results({Config.Actions.PASSWORD_PARAM_NAME: self.get_secret("app", key_name)})

    def _on_set_password(self, event: ActionEvent) -> None:
        """Set the password for the specified user."""
        # only leader can write the new password into peer relation.
        if not self.unit.is_leader():
            event.fail("The action can be run only on leader unit.")
            return

        username = self._get_user_or_fail_event(
            event, default_username=OperatorUser.get_username()
        )
        if not username:
            return

        new_password = event.params.get(Config.Actions.PASSWORD_PARAM_NAME, generate_password())

        if new_password == self.get_secret(
            "app", MonitorUser.get_password_key_name_for_user(username)
        ):
            event.log("The old and new passwords are equal.")
            event.set_results({Config.Actions.PASSWORD_PARAM_NAME: new_password})
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                mongo.set_user_password(username, new_password)
            except NotReadyError:
                event.fail(
                    "Failed to change the password: Not all members healthy or finished initial sync."
                )
                return
            except PyMongoError as e:
                event.fail(f"Failed changing the password: {e}")
                return

        self.set_secret("app", MongoDBUser.get_password_key_name_for_user(username), new_password)

        if username == MonitorUser.get_username():
            self._connect_mongodb_exporter()

        event.set_results({Config.Actions.PASSWORD_PARAM_NAME: new_password})

    # END: actions

    # BEGIN: user management
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

    # END: user management

    # BEGIN: helper functions

    def _is_user_created(self, user: MongoDBUser) -> bool:
        return f"{user.get_username()}-user-created" in self.app_peer_data

    def _set_user_created(self, user: MongoDBUser) -> None:
        self.app_peer_data[f"{user.get_username()}-user-created"] = "True"

    def _get_mongodb_config_for_user(
        self, user: MongoDBUser, hosts: list[str]
    ) -> MongoDBConfiguration:
        external_ca, _ = self.tls.get_tls_files("unit")
        internal_ca, _ = self.tls.get_tls_files("app")

        return MongoDBConfiguration(
            replset=self.app.name,
            database=user.get_database_name(),
            username=user.get_username(),
            password=self.get_secret("app", user.get_password_key_name()),
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

    def _check_or_set_user_password(self, user: MongoDBUser) -> None:
        key = user.get_password_key_name()
        if not self.get_secret("app", key):
            self.set_secret("app", key, generate_password())

    def _generate_secrets(self) -> None:
        """Generate passwords and put them into peer relation.

        The same keyFile and operator password on all members are needed.
        It means it is needed to generate them once and share between members.
        NB: only leader should execute this function.
        """
        self._check_or_set_user_password(OperatorUser)
        self._check_or_set_user_password(MonitorUser)

        if not self.get_secret("app", "keyfile"):
            self.set_secret("app", "keyfile", generate_keyfile())

    def _update_app_relation_data(self):
        """Helper function to update application relation data."""
        with MongoDBConnection(self.mongodb_config) as mongo:
            database_users = mongo.get_users()

        for relation in self.model.relations[Config.Relations.NAME]:
            username = self.client_relations._get_username_from_relation_id(relation.id)
            password = relation.data[self.app]["password"]
            if username in database_users:
                config = self.client_relations._get_config(username, password)
                relation.data[self.app].update(
                    {
                        "endpoints": ",".join(config.hosts),
                        "uris": config.uri,
                    }
                )

    def _update_hosts(self, event) -> None:
        """Update replica set hosts and remove any unremoved replicas from the config."""
        if not self._db_initialised:
            return

        self._process_unremoved_units(event)
        self.app_peer_data["replica_set_hosts"] = json.dumps(self._unit_hosts)

    def _process_unremoved_units(self, event: LeaderElectedEvent) -> None:
        """Removes replica set members that are no longer running as a juju hosts."""
        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members()
                for member in replset_members - self.mongodb_config.hosts:
                    logger.debug("Removing %s from replica set", member)
                    mongo.remove_replset_member(member)
            except NotReadyError:
                logger.info("Deferring process_unremoved_units: another member is syncing")
                event.defer()
            except PyMongoError as e:
                logger.error("Deferring process_unremoved_units: error=%r", e)
                event.defer()

    def _initialise_replica_set(self, event: StartEvent) -> None:
        """Initialise replica set and create users."""
        if self._db_initialised:
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
                logger.info("User initialization")
                self._init_operator_user()
                self._init_monitor_user()
                logger.info("Reconcile relations")
                self.client_relations.oversee_users(None, event)
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

            self._db_initialised = True

    def get_secret(self, scope: str, key: str) -> Optional[str]:
        """Get TLS secret from the secret storage."""
        if scope == "unit":
            return self.unit_peer_data.get(key, None)
        elif scope == "app":
            return self.app_peer_data.get(key, None)
        else:
            raise RuntimeError("Unknown secret scope.")

    def set_secret(self, scope: str, key: str, value: Optional[str]) -> None:
        """Get TLS secret from the secret storage."""
        if scope == "unit":
            if not value:
                del self.unit_peer_data[key]
                return
            self.unit_peer_data.update({key: value})
        elif scope == "app":
            if not value:
                del self.app_peer_data[key]
                return
            self.app_peer_data.update({key: value})
        else:
            raise RuntimeError("Unknown secret scope.")

    def _push_keyfile_to_workload(self, container: Container) -> None:
        """Upload the keyFile to a workload container."""
        container.push(
            Config.CONF_DIR + "/" + Config.TLS.KEY_FILE_NAME,
            self.get_secret("app", "keyfile"),
            make_dirs=True,
            permissions=0o400,
            user=Config.UNIX_USER,
            group=Config.UNIX_GROUP,
        )

    def _push_certificate_to_workload(self, container: Container) -> None:
        """Uploads certificate to the workload container."""
        external_ca, external_pem = self.tls.get_tls_files("unit")
        if external_ca is not None:
            logger.debug("Uploading external ca to workload container")
            container.push(
                Config.CONF_DIR + "/" + Config.TLS.EXT_CA_FILE,
                external_ca,
                make_dirs=True,
                permissions=0o400,
                user=Config.UNIX_USER,
                group=Config.UNIX_GROUP,
            )
        if external_pem is not None:
            logger.debug("Uploading external pem to workload container")
            container.push(
                Config.CONF_DIR + "/" + Config.TLS.EXT_PEM_FILE,
                external_pem,
                make_dirs=True,
                permissions=0o400,
                user=Config.UNIX_USER,
                group=Config.UNIX_GROUP,
            )

        internal_ca, internal_pem = self.tls.get_tls_files("app")
        if internal_ca is not None:
            logger.debug("Uploading internal ca to workload container")
            container.push(
                Config.CONF_DIR + "/" + Config.TLS.INT_CA_FILE,
                internal_ca,
                make_dirs=True,
                permissions=0o400,
                user=Config.UNIX_USER,
                group=Config.UNIX_GROUP,
            )
        if internal_pem is not None:
            logger.debug("Uploading internal pem to workload container")
            container.push(
                Config.CONF_DIR + "/" + Config.TLS.INT_PEM_FILE,
                internal_pem,
                make_dirs=True,
                permissions=0o400,
                user=Config.UNIX_USER,
                group=Config.UNIX_GROUP,
            )

    def _get_hostname_for_unit(self, unit: Unit) -> str:
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
        container = self.unit.get_container("mongod")

        if not container.can_connect():
            return

        # must wait for leader to set URI before connecting
        if not self.get_secret("app", MonitorUser.get_password_key_name()):
            return

        # Add initial Pebble config layer using the Pebble API
        # mongodb_exporter --mongodb.uri=
        container.add_layer("mongodb_exporter", self._monitor_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()

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
    def _fix_data_dir(container: Container) -> None:
        """Ensure the data directory for mongodb is writable for the "mongodb" user.

        Until the ability to set fsGroup and fsGroupChangePolicy via Pod securityContext
        is available, we fix permissions incorrectly with chown.
        """
        paths = container.list_files(Config.DATA_DIR, itself=True)
        assert len(paths) == 1, "list_files doesn't return only the directory itself"
        logger.debug(f"Data directory ownership: {paths[0].user}:{paths[0].group}")
        if paths[0].user != Config.UNIX_USER or paths[0].group != Config.UNIX_GROUP:
            container.exec(
                f"chown {Config.UNIX_USER}:{Config.UNIX_GROUP} -R {Config.DATA_DIR}".split(" ")
            )

    # END: static methods


if __name__ == "__main__":
    main(MongoDBCharm)
