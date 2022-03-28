#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd
# See LICENSE file for licensing details.

import logging

from ops.main import main
from ops.charm import CharmBase
from ops.model import ActiveStatus, WaitingStatus, BlockedStatus
from ops.pebble import Layer, PathError, ProtocolError, ExecError
from charms.mongodb_libs.v0.mongodb import (
    MongoDBConfiguration,
    MongoDBConnection,
)
from charms.mongodb_libs.v0.helpers import (
    KEY_FILE,
    get_mongod_cmd,
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    log_signal,
)

logger = logging.getLogger(__name__)
PEER = "mongodb"


class MongoDBCharm(CharmBase):
    """A Juju Charm to deploy MongoDB on Kubernetes."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.leader_elected, self._on_leader_elected)
        self.framework.observe(
            self.on.mongod_pebble_ready, self._on_mongod_pebble_ready
        )
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(
            self.on[PEER].relation_changed, self._reconfigure
        )
        self.framework.observe(
            self.on[PEER].relation_departed, self._reconfigure
        )

    ##############################################
    #           CHARM HOOKS HANDLERS             #
    ##############################################

    @log_signal
    def _on_leader_elected(self, _):
        """Assume leadership.

        admin password and keyFile should be created before running MongoDB
        _on_leader_elected runs before _mongod_pebble_ready.
        In this function each new leader checks if admin password and the
        keyFile is available in peer relation data. If not the leader sets
        these into peer relation data.
        """

        if "admin_password" not in self.app_data:
            self.app_data["admin_password"] = generate_password()

        if "keyfile" not in self.app_data:
            self.app_data["keyfile"] = generate_keyfile()

    @log_signal
    def _on_mongod_pebble_ready(self, event):
        """Configure MongoDB pebble layer specification."""

        # mongod needs keyFile on filesystem
        if not self.unit.get_container("mongod").can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return
        self.set_keyfile()

        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
        # Add initial Pebble config layer using the Pebble API
        container.add_layer("mongod", self._mongod_layer, combine=True)
        # Autostart any services that were defined with startup: enabled
        container.autostart()
        # TODO: rework status here
        self.unit.status = ActiveStatus()

    @log_signal
    def _on_start(self, event):
        """Initialize MongoDB.

        Initialization of replSet should be made once after start.
        MongoDB needs some time to become fully started.
        This event handler is deferred if initialization of MongoDB
        replica set fails. By doing so it is guaranteed that another
        attempt at initialization will be made.
        """
        if not self.unit.is_leader():
            return

        container = self.unit.get_container("mongod")
        if not container.can_connect():
            logger.debug("mongod container is not ready yet.")
            event.defer()
            return

        if "replset_initialized" not in self.app_data:
            self.unit.status = WaitingStatus("Initializing MongoDB")

            with MongoDBConnection(
                self.mongodb_config, "localhost", direct=True
            ) as direct_client:
                if not direct_client.is_ready:
                    logger.debug("mongodb service is not ready yet.")
                    event.defer()
                    return
                try:
                    logger.info("Replica Set initialization")
                    direct_client.init_replset()
                    logger.info("User initialization")
                    process = container.exec(
                        command=get_create_user_cmd(self.mongodb_config),
                        stdin=self.mongodb_config.admin_password,
                    )
                    stdout, _ = process.wait_output()
                    logger.debug("User created: %r", stdout)
                except ExecError as e:
                    logger.error(
                        f"Deferring on_start: exit code: {e.exit_code},"
                        f" stderr: {e.stderr}"
                    )
                    event.defer()
                    return
                except Exception as e:
                    logger.error(f"Deferring on_start since: error={e}")
                    event.defer()
                    return
                self.app_data["replset_initialized"] = "True"

    @log_signal
    def _reconfigure(self, event):
        """Reconfigure replicat set.

        The number of replicas in the MongoDB replica set is updated.
        """
        if not self.unit.is_leader():
            return

        if "replset_initialized" not in self.app_data:
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                if not mongo.is_replset_up_to_date:
                    logger.info("Reconfigure replica set")
                    mongo.reconfigure_replset()
            except Exception as e:
                logger.info("Deferring reconfigure since : error={}".format(e))
                event.defer()

    ##############################################
    #               PROPERTIES                   #
    ##############################################

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
    def app_data(self):
        """Peer relation data object"""
        return self.model.get_relation(PEER).data[self.app]

    @property
    def mongodb_config(self) -> MongoDBConfiguration:
        """Construct a configuration object with settings
        needed for correct handling interactions with MongoDB.

        Returns:
            A MongoDBConfiguration object
        """
        peers = self.model.get_relation(PEER)
        hosts = [self.get_hostname_by_unit(self.unit.name)] + [
            self.get_hostname_by_unit(unit.name) for unit in peers.units
        ]

        return MongoDBConfiguration(
            replset_name=self.app.name,
            admin_user="operator",
            admin_password=self.app_data.get("admin_password"),
            hosts=hosts,
            sharding=False,
        )

    ##############################################
    #             UTILITY METHODS                #
    ##############################################

    def set_keyfile(self):
        """Upload the keyFile to a workload container."""
        try:
            self.unit.get_container("mongod").push(
                KEY_FILE,
                self.app_data.get("keyfile"),
                permissions=0o400,
                user="mongodb",
                group="mongodb",
            )
        except (PathError, ProtocolError):
            self.unit.status = BlockedStatus("Failed to set security key")

    def get_hostname_by_unit(self, unit_name: str) -> str:
        """Construct a DNS name for a MongoDB unit.

        Args:
            unit_name: the juju unit name, e.g. "mongodb/1".

        Returns:
            A string representing the hostname of the MongoDB unit.
        """
        unit_id = unit_name.split("/")[1]
        return f"{self.app.name}-{unit_id}.{self.app.name}-endpoints"


if __name__ == "__main__":
    main(MongoDBCharm)
