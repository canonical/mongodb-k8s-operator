#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm for MongoDB on Kubernetes.

Run the developer's favourite document database — MongoDB! Charm for MongoDB is a fully supported,
automated solution from Canonical for running production-grade MongoDB on Kubernetes. It offers
a simple, secure and highly available setup with automatic recovery on fail-over. The solution
includes scaling and other capabilities.
"""

import logging
from typing import Dict, Optional

from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v0.loki_push_api import LogProxyConsumer
from charms.mongodb.v0.helpers import (
    CONF_DIR,
    DATA_DIR,
    KEY_FILE,
    MONGODB_LOG_FILENAME,
    TLS_EXT_CA_FILE,
    TLS_EXT_PEM_FILE,
    TLS_INT_CA_FILE,
    TLS_INT_PEM_FILE,
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    get_mongod_args,
)
from charms.mongodb.v0.mongodb import (
    CHARM_USERS,
    MongoDBConfiguration,
    MongoDBConnection,
    NotReadyError,
)
from charms.mongodb.v0.mongodb_provider import MongoDBProvider
from charms.mongodb.v0.mongodb_tls import MongoDBTLS
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, Container
from ops.pebble import ExecError, Layer, PathError, ProtocolError
from pymongo.errors import PyMongoError
from tenacity import before_log, retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)
PEER = "database-peers"
MONITOR_PRIVILEGES = {
    "resource": {"db": "", "collection": ""},
    "actions": ["listIndexes", "listCollections", "dbStats", "dbHash", "collStats", "find"],
}
UNIX_USER = "mongodb"
UNIX_GROUP = "mongodb"
MONGODB_EXPORTER_PORT = 9216
REL_NAME = "database"


class MongoDBCharm(CharmBase):
    """A Juju Charm to deploy MongoDB on Kubernetes."""

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.mongod_pebble_ready, self.on_mongod_pebble_ready)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.leader_elected, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_changed, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_departed, self._reconfigure)
        self.framework.observe(self.on.get_password_action, self._on_get_password)
        self.framework.observe(self.on.set_password_action, self._on_set_password)

        self.client_relations = MongoDBProvider(self)
        self.tls = MongoDBTLS(self, PEER)

        self.metrics_endpoint = MetricsEndpointProvider(
            self,
            refresh_event=self.on.start,
            jobs=[{"static_configs": [{"targets": [f"*:{MONGODB_EXPORTER_PORT}"]}]}],
        )
        self.grafana_dashboards = GrafanaDashboardProvider(self)
        self.loki_push = LogProxyConsumer(
            self,
            log_files=[f"{DATA_DIR}/{MONGODB_LOG_FILENAME}"],
            relation_name="logging",
            container_name="mongod",
        )

    # BEGIN: properties
    @property
    def mongodb_config(self) -> MongoDBConfiguration:
        """Create a configuration object with settings.

        Needed for correct handling interactions with MongoDB.

        Returns:
            A MongoDBConfiguration object
        """
        peers = self.model.get_relation(PEER)
        hosts = [self.get_hostname_by_unit(self.unit.name)] + [
            self.get_hostname_by_unit(unit.name) for unit in peers.units
        ]
        external_ca, _ = self.tls.get_tls_files("unit")
        internal_ca, _ = self.tls.get_tls_files("app")

        return MongoDBConfiguration(
            replset=self.app.name,
            database="admin",
            username="operator",
            password=self.get_secret("app", "operator_password"),
            hosts=set(hosts),
            roles={"default"},
            tls_external=external_ca is not None,
            tls_internal=internal_ca is not None,
        )
    
    @property
    def monitor_config(self) -> MongoDBConfiguration:
        """Generates a MongoDBConfiguration object for this deployment of MongoDB."""
        return MongoDBConfiguration(
            replset=self.app.name,
            database="",
            username="monitor",
            # MongoDB Exporter can only connect to one replica - not the entire set.
            password=self.get_secret("app", "monitor_password"),
            hosts={self.get_hostname_by_unit(self.unit.name)},
            roles={"monitor"},
            tls_external=self.tls.get_tls_files("unit") is not None,
            tls_internal=self.tls.get_tls_files("app") is not None,
        )

    @property
    def _monitor_layer(self) -> Layer:
        """Returns a Pebble configuration layer for mongod."""
        layer_config = {
            "summary": "mongodb_exporter layer",
            "description": "Pebble config layer for mongodb_exporter",
            "services": {
                "mongodb_exporter": {
                    "override": "replace",
                    "summary": "mongodb_exporter",
                    "command": "mongodb_exporter --collector.diagnosticdata --compatible-mode",
                    "startup": "enabled",
                    "user": UNIX_USER,
                    "group": UNIX_GROUP,
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
                    "user": UNIX_USER,
                    "group": UNIX_GROUP,
                }
            },
        }
        return Layer(layer_config)
    
    @property
    def unit_peer_data(self) -> Dict:
        """Peer relation data object."""
        relation = self.model.get_relation(PEER)
        if relation is None:
            return {}

        return relation.data[self.unit]

    @property
    def app_peer_data(self) -> Dict:
        """Peer relation data object."""
        relation = self.model.get_relation(PEER)
        if relation is None:
            return {}

        return relation.data[self.app]

    # END: properties

    # BEGIN: charm events
    def on_mongod_pebble_ready(self, event) -> None:
        """Configure MongoDB pebble layer specification."""
        # Get a reference the container attribute
        container = self.unit.get_container("mongod")
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
        services = container.get_services("mongod")
        if services and services["mongod"].is_running():
            new_command = "mongod " + get_mongod_args(self.mongodb_config)
            cur_command = container.get_plan().services["mongod"].command
            if new_command != cur_command:
                logger.debug("restart MongoDB due to arguments change: %s", new_command)
                container.stop("mongod")

        # Add initial Pebble config layer using the Pebble API
        container.add_layer("mongod", self._mongod_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()

        # when a network cuts and the pod restarts - reconnect to the exporter
        self._connect_mongodb_exporter()

        # TODO: rework status
        self.unit.status = ActiveStatus()
    
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
        if not self.unit.is_leader():
            return

        container = self.unit.get_container("mongod")
        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return

        if not container.exists("/tmp/mongodb-27017.sock"):
            logger.debug("The mongod socket is not ready yet.")
            event.defer()
            return

        if "db_initialised" in self.app_peer_data:
            # The replica set should be initialised only once. Check should be
            # external (e.g., check initialisation inside peer relation). We
            # shouldn't rely on MongoDB response because the data directory
            # can be corrupted.
            return

        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            if not direct_mongo.is_ready:
                logger.debug("mongodb service is not ready yet.")
                event.defer()
                return
            try:
                logger.info("Replica Set initialization")
                direct_mongo.init_replset()
                logger.info("User initialization")
                self._init_user(container)
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

            self.app_peer_data["db_initialised"] = "True"

    def _reconfigure(self, event) -> None:
        """Reconfigure replica set.

        The amount replicas in the MongoDB replica set is updated.
        """
        self._connect_mongodb_exporter()

        if not self.unit.is_leader():
            return

        # Admin password and keyFile should be created before running MongoDB.
        # This code runs on leader_elected event before mongod_pebble_ready
        self._generate_passwords()

        # reconfiguration can be successful only if a replica set is initialised.
        if "db_initialised" not in self.app_peer_data:
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members()

                # compare sets of mongod replica set members and juju hosts
                # to avoid unnecessary reconfiguration.
                if replset_members == self.mongodb_config.hosts:
                    return

                # remove members first, it is faster
                logger.info("Reconfigure replica set")
                for member in replset_members - self.mongodb_config.hosts:
                    logger.debug("Removing %s from the replica set", member)
                    mongo.remove_replset_member(member)
                for member in self.mongodb_config.hosts - replset_members:
                    logger.debug("Adding %s to the replica set", member)
                    with MongoDBConnection(
                        self.mongodb_config, member, direct=True
                    ) as direct_mongo:
                        if not direct_mongo.is_ready:
                            logger.debug("Deferring reconfigure: %s is not ready yet.", member)
                            event.defer()
                            return
                    mongo.add_replset_member(member)

                # app relations should be made aware of the new set of hosts
                self._update_app_relation_data(mongo.get_users())
            except NotReadyError:
                logger.info("Deferring reconfigure: another member doing sync right now")
                event.defer()
            except PyMongoError as e:
                logger.info("Deferring reconfigure: error=%r", e)
                event.defer()

    # END: charm events

    # BEGIN: actions

    def _on_get_password(self, event: ActionEvent) -> None:
        """Returns the password for the user as an action response."""
        username = "operator"
        if "username" in event.params:
            username = event.params["username"]
        if username not in CHARM_USERS:
            event.fail(
                f"The action can be run only for users used by the charm: {CHARM_USERS} not {username}"
            )
            return
        event.set_results({"password": self.get_secret("app", f"{username}_password")})

    def _on_set_password(self, event: ActionEvent) -> None:
        """Set the password for the specified user."""
        # only leader can write the new password into peer relation.
        if not self.unit.is_leader():
            event.fail("The action can be run only on leader unit.")
            return

        username = "operator"
        if "username" in event.params:
            username = event.params["username"]
        if username not in CHARM_USERS:
            event.fail(
                f"The action can be run only for users used by the charm: {CHARM_USERS} not {username}."
            )
            return

        new_password = generate_password()
        if "password" in event.params:
            new_password = event.params["password"]

        if new_password == self.get_secret("app", f"{username}_password"):
            event.log("The old and new passwords are equal.")
            event.set_results({"password": new_password})
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
        self.set_secret("app", f"{username}_password", new_password)

        if username == "monitor":
            self._connect_mongodb_exporter()

        event.set_results({"password": new_password})
    # END: actions

    def _generate_passwords(self) -> None:
        """Generate passwords and put them into peer relation.

        The same keyFile and operator password on all members are needed.
        It means it is needed to generate them once and share between members.
        NB: only leader should execute this function.
        """
        if not self.get_secret("app", "operator_password"):
            self.set_secret("app", "operator_password", generate_password())

        if not self.get_secret("app", "monitor_password"):
            self.set_secret("app", "monitor_password", generate_password())

        if not self.get_secret("app", "keyfile"):
            self.set_secret("app", "keyfile", generate_keyfile())

    def _update_app_relation_data(self, database_users):
        """Helper function to update application relation data."""
        for relation in self.model.relations[REL_NAME]:
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
                license_file = container.pull(path=f"/licenses/LICENSE-{license_name}")
                f = open("LICENSE", "x")
                f.write(str(license_file.read()))
                f.close()
            except FileExistsError:
                pass

    def _push_keyfile_to_workload(self, container: Container) -> None:
        """Upload the keyFile to a workload container."""
        container.push(
            CONF_DIR + "/" + KEY_FILE,
            self.get_secret("app", "keyfile"),
            make_dirs=True,
            permissions=0o400,
            user=UNIX_USER,
            group=UNIX_GROUP,
        )

    def _push_certificate_to_workload(self, container: Container) -> None:
        """Uploads certificate to the workload container."""
        external_ca, external_pem = self.tls.get_tls_files("unit")
        if external_ca is not None:
            logger.debug("Uploading external ca to workload container")
            container.push(
                CONF_DIR + "/" + TLS_EXT_CA_FILE,
                external_ca,
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
        if external_pem is not None:
            logger.debug("Uploading external pem to workload container")
            container.push(
                CONF_DIR + "/" + TLS_EXT_PEM_FILE,
                external_pem,
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )

        internal_ca, internal_pem = self.tls.get_tls_files("app")
        if internal_ca is not None:
            logger.debug("Uploading internal ca to workload container")
            container.push(
                CONF_DIR + "/" + TLS_INT_CA_FILE,
                internal_ca,
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )
        if internal_pem is not None:
            logger.debug("Uploading internal pem to workload container")
            container.push(
                CONF_DIR + "/" + TLS_INT_PEM_FILE,
                internal_pem,
                make_dirs=True,
                permissions=0o400,
                user=UNIX_USER,
                group=UNIX_GROUP,
            )

    @staticmethod
    def _fix_data_dir(container: Container) -> None:
        """Ensure the data directory for mongodb is writable for the "mongodb" user.

        Until the ability to set fsGroup and fsGroupChangePolicy via Pod securityContext
        is available, we fix permissions incorrectly with chown.
        """
        paths = container.list_files(DATA_DIR, itself=True)
        assert len(paths) == 1, "list_files doesn't return only the directory itself"
        logger.debug(f"Data directory ownership: {paths[0].user}:{paths[0].group}")
        if paths[0].user != UNIX_USER or paths[0].group != UNIX_GROUP:
            container.exec(f"chown {UNIX_USER}:{UNIX_GROUP} -R {DATA_DIR}".split(" "))

    def get_hostname_by_unit(self, unit_name: str) -> str:
        """Create a DNS name for a MongoDB unit.

        Args:
            unit_name: the juju unit name, e.g. "mongodb/1".

        Returns:
            A string representing the hostname of the MongoDB unit.
        """
        unit_id = unit_name.split("/")[1]
        return f"{self.app.name}-{unit_id}.{self.app.name}-endpoints"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_user(self, container: Container) -> None:
        """Creates initial operator user for MongoDB.

        Initial operator user can be created only through localhost connection.
        see https://www.mongodb.com/docs/manual/core/localhost-exception/
        unfortunately, pymongo unable to create a connection that is considered
        as local connection by MongoDB, even if a socket connection is used.
        As a result, there are only hackish ways to create initial user.
        It is needed to install mongodb-clients inside the charm container
        to make this function work correctly.
        """
        if "user_created" in self.app_peer_data:
            return

        mongo_cmd = (
            "/usr/bin/mongosh" if container.exists("/usr/bin/mongosh") else "/usr/bin/mongo"
        )

        process = container.exec(
            command=get_create_user_cmd(self.mongodb_config, mongo_cmd),
            stdin=self.mongodb_config.password,
        )
        stdout, _ = process.wait_output()
        logger.debug("User created: %s", stdout)

        self.app_peer_data["user_created"] = "True"

    def _connect_mongodb_exporter(self) -> None:
        """Exposes the endpoint to mongodb_exporter."""
        container = self.unit.get_container("mongod")

        if not container.can_connect():
            return

        # must wait for leader to set URI before connecting
        if not self.get_secret("app", "monitor_password"):
            return

        # Add initial Pebble config layer using the Pebble API
        # mongodb_exporter --mongodb.uri=
        container.add_layer("mongodb_exporter", self._monitor_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_monitor_user(self):
        """Creates the monitor user on the MongoDB database."""
        if "monitor_user_created" in self.app_peer_data:
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            logger.debug("creating the monitor user roles...")
            mongo.create_role(role_name="explainRole", privileges=MONITOR_PRIVILEGES)
            logger.debug("creating the monitor user...")
            mongo.create_user(self.monitor_config)
            self._connect_mongodb_exporter()
            self.app_peer_data["monitor_user_created"] = "True"




if __name__ == "__main__":
    main(MongoDBCharm)
