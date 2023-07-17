#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
high availability of the MongoDB charm.
"""

import logging
import os
import signal
import subprocess
from typing import Dict, Optional

from charms.data_platform_libs.v0.database_requires import DatabaseRequires
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, Relation, WaitingStatus
from pymongo import MongoClient
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)

DATABASE_NAME = "continuous_writes_database"
PEER = "application-peers"
LAST_WRITTEN_FILE = "last_written_value"
PROC_PID_KEY = "proc-pid"


class ContinuousWritesApplication(CharmBase):
    """Application charm that continuously writes to MongoDB."""

    def __init__(self, *args):
        super().__init__(*args)

        # Charm events
        self.framework.observe(self.on.start, self._on_start)

        self.framework.observe(
            self.on.clear_continuous_writes_action, self._on_clear_continuous_writes_action
        )
        self.framework.observe(
            self.on.start_continuous_writes_action, self._on_start_continuous_writes_action
        )
        self.framework.observe(
            self.on.stop_continuous_writes_action, self._on_stop_continuous_writes_action
        )

        # Database related events
        self.database = DatabaseRequires(self, "database", DATABASE_NAME)
        self.framework.observe(self.database.on.database_created, self._on_database_created)

    # ==============
    # Properties
    # ==============

    @property
    def _peers(self) -> Optional[Relation]:
        """Retrieve the peer relation (`ops.model.Relation`)."""
        return self.model.get_relation(PEER)

    @property
    def app_peer_data(self) -> Dict:
        """Application peer relation data object."""
        if self._peers is None:
            return {}

        return self._peers.data[self.app]

    @property
    def _database_config(self):
        """Returns the database config to use to connect to the MongoDB cluster."""
        data = list(self.database.fetch_relation_data().values())[0]
        username, password, endpoints, replset, uris = (
            data.get("username"),
            data.get("password"),
            data.get("endpoints"),
            data.get("replset"),
            data.get("uris"),
        )
        if None in [username, password, endpoints, replset, uris]:
            return {}

        return {
            "user": username,
            "password": password,
            "endpoints": endpoints,
            "replset": replset,
            "uris": uris,
        }

    # ==============
    # Helpers
    # ==============

    def _start_continuous_writes(self, starting_number: int) -> None:
        """Start continuous writes to the MongoDB cluster."""
        if not self._database_config:
            return

        self._stop_continuous_writes()

        # Run continuous writes in the background
        proc = subprocess.Popen(
            [
                "/usr/bin/python3",
                "src/continuous_writes.py",
                self._database_config["uris"],
                str(starting_number),
            ]
        )

        # Store the continuous writes process id in stored state to be able to stop it later
        self.app_peer_data[PROC_PID_KEY] = str(proc.pid)

    def _stop_continuous_writes(self) -> Optional[int]:
        """Stop continuous writes to the MongoDB cluster and return the last written value."""
        if not self._database_config:
            return None

        if not self.app_peer_data.get(PROC_PID_KEY):
            return None

        # Send a SIGTERM to the process and wait for the process to exit
        try:
            os.kill(int(self.app_peer_data[PROC_PID_KEY]), signal.SIGTERM)
        except ProcessLookupError:
            logger.info(f"Process {PROC_PID_KEY} was killed already (or never existed)")

        del self.app_peer_data[PROC_PID_KEY]

        # read the last written_value
        try:
            for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(5)):
                with attempt:
                    with open(LAST_WRITTEN_FILE, "r") as fd:
                        last_written_value = int(fd.read())
        except RetryError as e:
            logger.exception("Unable to query the database", exc_info=e)
            return -1

        os.remove(LAST_WRITTEN_FILE)
        return last_written_value

    # ==============
    # Handlers
    # ==============

    def _on_start(self, _) -> None:
        """Handle the start event."""
        self.unit.status = WaitingStatus()

    def _on_clear_continuous_writes_action(self, _) -> None:
        """Handle the clear continuous writes action event."""
        if not self._database_config:
            return

        self._stop_continuous_writes()

        client = MongoClient(self._database_config["uris"])
        db = client[DATABASE_NAME]

        # collection for continuous writes
        test_collection = db["test_collection"]
        test_collection.drop()

        # collection for replication tests
        test_collection = db["test_ubuntu_collection"]
        test_collection.drop()

        client.close()

    def _on_start_continuous_writes_action(self, _) -> None:
        """Handle the start continuous writes action event."""
        if not self._database_config:
            return

        self._start_continuous_writes(1)

    def _on_stop_continuous_writes_action(self, event: ActionEvent) -> None:
        """Handle the stop continuous writes action event."""
        if not self._database_config:
            return event.set_results({"writes": -1})

        writes = self._stop_continuous_writes()
        event.set_results({"writes": writes or -1})

    def _on_database_created(self, _) -> None:
        """Handle the database created event."""
        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(ContinuousWritesApplication)
