#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm for MongoDB on Kubernetes.

Run the developer's favourite document database - MongoDB! Charm for MongoDB is a fully supported,
automated solution from Canonical for running production-grade MongoDB on Kubernetes. It offers
simple, secure and highly available setup with automatic recovery on failover. The solution
includes scaling and other capabilities.
"""

import logging
from typing import Dict

from charms.mongodb_libs.v0.helpers import (
    KEY_FILE,
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    get_mongod_cmd,
)
from charms.mongodb_libs.v0.mongodb import (
    MongoDBConfiguration,
    MongoDBConnection,
    NotReadyError,
)
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus, Container
from ops.pebble import ExecError, Layer, PathError, ProtocolError
from pymongo.errors import PyMongoError
from tenacity import before_log, retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)
PEER = "mongodb"


class MongoDBCharm(CharmBase):
    """A Juju Charm to deploy MongoDB on Kubernetes."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self.on.mongod_pebble_ready, self._on_mongod_pebble_ready)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on[PEER].relation_changed, self._reconfigure)
        self.framework.observe(self.on[PEER].relation_departed, self._reconfigure)

    def _on_leader_elected(self, _) -> None:
        """Assume leadership.

        admin password and keyFile should be created before running MongoDB
        _on_leader_elected runs before _mongod_pebble_ready.
        In this function each new leader checks if admin password and the
        keyFile is available in peer relation data. If not the leader sets
        these into peer relation data.
        """
        if "admin_password" not in self._app_data:
            self._app_data["admin_password"] = generate_password()

        if "keyfile" not in self._app_data:
            self._app_data["keyfile"] = generate_keyfile()

    def _on_mongod_pebble_ready(self, event) -> None:
        """Configure MongoDB pebble layer specification."""
        # mongod needs keyFile on filesystem
        container = self.unit.get_container("mongod")
        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return
        try:
            self._set_keyfile(container)
        except (PathError, ProtocolError) as e:
            logger.error("Cannot put keyFile: %r", e)
            event.defer()
            return

        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
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

        Initial admin user can be created only through localhost connection.
        see https://www.mongodb.com/docs/manual/core/localhost-exception/
        unfortunately, pymongo not able to create connection which considered
        as local connection by MongoDB, even if socket connection used.
        As result where are only hackish ways to create initial user.
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

        if "replset_initialized" not in self._app_data:
            with MongoDBConnection(self._mongodb_config, "localhost", direct=True) as direct_mongo:
                if not direct_mongo.is_ready:
                    logger.debug("mongodb service is not ready yet.")
                    event.defer()
                    return
                try:
                    logger.info("Replica Set initialization")
                    direct_mongo.init_replset()
                    logger.info("User initialization")
                    self._init_user(container)

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
                self._app_data["replset_initialized"] = "True"

    def _reconfigure(self, event) -> None:
        """Reconfigure replicat set.

        The number of replicas in the MongoDB replica set is updated.
        """
        if not self.unit.is_leader():
            return

        if "replset_initialized" not in self._app_data:
            return

        with MongoDBConnection(self._mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members
                if replset_members != self._mongodb_config.hosts:  # compare sets
                    logger.info("Reconfigure replica set")
                    # remove members first, it is faster
                    for member in replset_members - self._mongodb_config.hosts:
                        logger.debug("Removing %s from replica set", member)
                        mongo.remove_replset_member(member)
                    for member in self._mongodb_config.hosts - replset_members:
                        logger.debug("Adding %s to replica set", member)
                        mongo.add_replset_member(member)
            except NotReadyError:
                logger.info("Deferring reconfigure since: another member doing sync right now")
                event.defer()
            except PyMongoError as e:
                logger.info("Deferring reconfigure since: error=%r", e)
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
                    "command": get_mongod_cmd(self._mongodb_config),
                    "startup": "enabled",
                    "user": "mongodb",
                    "group": "mongodb",
                }
            },
        }
        return Layer(layer_config)

    @property
    def _app_data(self) -> Dict:
        """Peer relation data object."""
        return self.model.get_relation(PEER).data[self.app]

    @property
    def _mongodb_config(self) -> MongoDBConfiguration:
        """Construct a configuration object with settings.

        Needed for correct handling interactions with MongoDB.

        Returns:
            A MongoDBConfiguration object
        """
        peers = self.model.get_relation(PEER)
        hosts = [self._get_hostname_by_unit(self.unit.name)] + [
            self._get_hostname_by_unit(unit.name) for unit in peers.units
        ]

        return MongoDBConfiguration(
            replset_name=self.app.name,
            admin_user="operator",
            admin_password=self._app_data.get("admin_password"),
            hosts=set(hosts),
            sharding=False,
        )

    def _set_keyfile(self, container: Container) -> None:
        """Upload the keyFile to a workload container."""
        container.push(
            KEY_FILE,
            self._app_data.get("keyfile"),
            make_dirs=True,
            permissions=0o400,
            user="mongodb",
            group="mongodb",
        )

    def _get_hostname_by_unit(self, unit_name: str) -> str:
        """Construct a DNS name for a MongoDB unit.

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
        """Creates initial admin user for MongoDB.

        Initial admin user can be created only through localhost connection.
        see https://www.mongodb.com/docs/manual/core/localhost-exception/
        unfortunately, pymongo not able to create connection which considered
        as local connection by MongoDB, even if socket connection used.
        As result where are only hackish ways to create initial user.
        It is needed to install mongodb-clients inside charm container to make
        this function work correctly
        """
        process = container.exec(
            command=get_create_user_cmd(self._mongodb_config),
            stdin=self._mongodb_config.admin_password,
        )
        stdout, _ = process.wait_output()
        logger.debug("User created: %s", stdout)


if __name__ == "__main__":
    main(MongoDBCharm)
