#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm for MongoDB on Kubernetes.

Run the developer's favourite document database — MongoDB! Charm for MongoDB is a fully supported,
automated solution from Canonical for running production-grade MongoDB on Kubernetes. It offers
simple, secure and highly available setup with automatic recovery on fail-over. The solution
includes scaling and other capabilities.
"""

import logging
from typing import Dict, Optional

from charms.mongodb_libs.v0.helpers import (
    KEY_FILE,
    TLS_EXT_CA_FILE,
    TLS_EXT_PEM_FILE,
    TLS_INT_CA_FILE,
    TLS_INT_PEM_FILE,
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    get_mongod_cmd,
)
from charms.mongodb_libs.v0.mongodb import (
    CHARM_USERS,
    MongoDBConfiguration,
    MongoDBConnection,
    NotReadyError,
)
from charms.mongodb_libs.v0.mongodb_provider import MongoDBProvider
from charms.mongodb_libs.v0.mongodb_tls import MongoDBTLS
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, Container
from ops.pebble import ExecError, Layer, PathError, ProtocolError
from pymongo.errors import PyMongoError
from tenacity import before_log, retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)
PEER = "database-peers"


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

    def _generate_passwords(self) -> None:
        """Generate passwords and put them into peer relation.

        The same keyFile and operator password on all members needed.
        It means, it is needed to generate them once and share between members.
        NB: only leader should execute this function.
        """
        if not self.get_secret("app", "operator_password"):
            self.set_secret("app", "operator_password", generate_password())

        if not self.get_secret("app", "keyfile"):
            self.set_secret("app", "keyfile", generate_keyfile())

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
        except (PathError, ProtocolError) as e:
            logger.error("Cannot put keyFile: %r", e)
            event.defer()
            return

        # service is running if TLS encryption enabled/disabled
        services = container.get_services("mongod")
        if services and services["mongod"].is_running():
            new_command = get_mongod_cmd(self.mongodb_config)
            cur_command = container.get_plan().services["mongod"].command
            if new_command != cur_command:
                logger.debug("restart MongoDB due to arguments change: %s", new_command)
                container.stop("mongod")

        # Add initial Pebble config layer using the Pebble API
        container.add_layer("mongod", self._mongod_layer, combine=True)
        # Restart changed services and start startup-enabled services.
        container.replan()
        # TODO: rework status
        self.unit.status = ActiveStatus()

    def _on_start(self, event) -> None:
        """Initialize MongoDB.

        Initialization of replSet should be made once after start.
        MongoDB needs some time to become fully started.
        This event handler is deferred if initialization of MongoDB
        replica set fails. By doing so it is guaranteed that another
        attempt at initialization will be made.

        Initial operator user can be created only through localhost connection.
        see https://www.mongodb.com/docs/manual/core/localhost-exception/
        unfortunately, pymongo unable to create connection that considered
        as local connection by MongoDB, even if socket connection used.
        As result, where are only hackish ways to create initial user.
        It is needed to install mongodb-clients inside charm container to make
        this function work correctly.
        """
        if not self.unit.is_leader():
            return

        container = self.unit.get_container("mongod")
        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return

        if not container.exists("/tmp/mongodb-27017.sock"):
            logger.debug("mongod socket is not ready yet.")
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
        """Reconfigure replicat set.

        The amount replicas in the MongoDB replica set is updated.
        """
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

                # compare set of mongod replica set members and juju hosts
                # to avoid the unnecessary reconfiguration.
                if replset_members == self.mongodb_config.hosts:
                    return

                # remove members first, it is faster
                logger.info("Reconfigure replica set")
                for member in replset_members - self.mongodb_config.hosts:
                    logger.debug("Removing %s from replica set", member)
                    mongo.remove_replset_member(member)
                for member in self.mongodb_config.hosts - replset_members:
                    logger.debug("Adding %s to replica set", member)
                    with MongoDBConnection(
                        self.mongodb_config, member, direct=True
                    ) as direct_mongo:
                        if not direct_mongo.is_ready:
                            logger.debug("Deferring reconfigure: %s is not ready yet.", member)
                            event.defer()
                            return
                    mongo.add_replset_member(member)
            except NotReadyError:
                logger.info("Deferring reconfigure: another member doing sync right now")
                event.defer()
            except PyMongoError as e:
                logger.info("Deferring reconfigure: error=%r", e)
                event.defer()

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
                    "command": get_mongod_cmd(self.mongodb_config),
                    "startup": "enabled",
                    "user": "mongodb",
                    "group": "mongodb",
                }
            },
        }
        return Layer(layer_config)

    @property
    def app_peer_data(self) -> Dict:
        """Peer relation data object."""
        relation = self.model.get_relation(PEER)
        if relation is None:
            return {}

        return relation.data[self.app]

    @property
    def unit_peer_data(self) -> Dict:
        """Peer relation data object."""
        relation = self.model.get_relation(PEER)
        if relation is None:
            return {}

        return relation.data[self.unit]

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

    def _push_keyfile_to_workload(self, container: Container) -> None:
        """Upload the keyFile to a workload container."""
        container.push(
            KEY_FILE,
            self.get_secret("app", "keyfile"),
            make_dirs=True,
            permissions=0o400,
            user="mongodb",
            group="mongodb",
        )

    def _push_certificate_to_workload(self, container: Container) -> None:
        """Uploads certificate to the workload container."""
        external_ca, external_pem = self.tls.get_tls_files("unit")
        if external_ca is not None:
            container.push(
                TLS_EXT_CA_FILE,
                external_ca,
                make_dirs=True,
                permissions=0o400,
                user="mongodb",
                group="mongodb",
            )
        if external_pem is not None:
            container.push(
                TLS_EXT_PEM_FILE,
                external_pem,
                make_dirs=True,
                permissions=0o400,
                user="mongodb",
                group="mongodb",
            )

        internal_ca, internal_pem = self.tls.get_tls_files("app")
        if internal_ca is not None:
            container.push(
                TLS_INT_CA_FILE,
                internal_ca,
                make_dirs=True,
                permissions=0o400,
                user="mongodb",
                group="mongodb",
            )
        if internal_pem is not None:
            container.push(
                TLS_INT_PEM_FILE,
                internal_pem,
                make_dirs=True,
                permissions=0o400,
                user="mongodb",
                group="mongodb",
            )

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
        unfortunately, pymongo unable to create connection that considered
        as local connection by MongoDB, even if socket connection used.
        As a result, where are only hackish ways to create initial user.
        It is needed to install mongodb-clients inside charm container to make
        this function work correctly.
        """
        if "user_created" in self.app_peer_data:
            return

        mongo_cmd = (
            "/usr/bin/mongosh" if container.exists("/usr/bin/mongosh") else "/usr/bin/mongo"
        )

        process = container.exec(
            command=get_create_user_cmd(mongo_cmd, self.mongodb_config),
            stdin=self.mongodb_config.password,
        )
        stdout, _ = process.wait_output()
        logger.debug("User created: %s", stdout)

        self.app_peer_data["user_created"] = "True"

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
        event.set_results({f"{username}-password": self.get_secret("app", f"{username}_password")})

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
            event.set_results({f"{username}-password": new_password})
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                mongo.set_user_password(username, new_password)
            except NotReadyError:
                event.fail(
                    "Failed changing the password: Not all members healthy or finished initial sync."
                )
                return
            except PyMongoError as e:
                event.fail(f"Failed changing the password: {e}")
                return
        self.set_secret("app", f"{username}_password", new_password)
        event.set_results({f"{username}-password": new_password})


if __name__ == "__main__":
    main(MongoDBCharm)
