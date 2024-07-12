#!/usr/bin/env python3
"""Code for handing statuses in the app and unit."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import json

from ops.charm import CharmBase
from ops.framework import Object
from ops.model import ActiveStatus, StatusBase, WaitingStatus

from config import Config

# The unique Charmhub library identifier, never change it
LIBID = "9b0b9fac53244229aed5ffc5e62141eb"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 2


class MongoDBStatusHandler(Object):
    """Verifies versions across multiple integrated applications."""

    def __init__(
        self,
        charm: CharmBase,
    ) -> None:
        """Constructor for CrossAppVersionChecker.

        Args:
            charm: charm to inherit from.
        """
        super().__init__(charm, None)
        self.charm = charm

        # TODO Future PR: handle update_status

    # BEGIN Helpers

    def set_and_share_status(self, status: StatusBase):
        """Sets the charm status and shares to app status and config-server if applicable."""
        # TODO Future Feature/Epic: process other statuses, i.e. only set provided status if its
        # appropriate.
        self.charm.unit.status = status

        self.set_app_status()

        if self.charm.is_role(Config.Role.SHARD):
            self.share_status_to_config_server()

    def set_app_status(self):
        """TODO Future Feature/Epic: parse statuses and set a status for the entire app."""

    def are_all_units_ready_for_upgrade(self) -> bool:
        """Returns True if all charm units status's show that they are ready for upgrade."""
        goal_state = self.charm.model._backend._run(
            "goal-state", return_output=True, use_json=True
        )
        is_different_revision = self.charm.get_cluster_mismatched_revision_status()
        for _, unit_state in goal_state["units"].items():
            if unit_state["status"] == "active":
                continue
            if unit_state["status"] != "waiting":
                return False

            if not is_different_revision:
                return False

        return True

    def are_shards_status_ready_for_upgrade(self) -> bool:
        """Returns True if all integrated shards status's show that they are ready for upgrade.

        A shard is ready for upgrade if it is either in the waiting for upgrade status or active
        status.
        """
        if not self.charm.is_role(Config.Role.CONFIG_SERVER):
            return False

        for sharding_relation in self.charm.config_server.get_all_sharding_relations():
            for unit in sharding_relation.units:
                unit_data = sharding_relation.data[unit]
                status_ready_for_upgrade = json.loads(
                    unit_data.get(Config.Status.STATUS_READY_FOR_UPGRADE, None)
                )
                if not status_ready_for_upgrade:
                    return False

        return True

    def share_status_to_config_server(self):
        """Shares this shards status info to the config server."""
        if not self.charm.is_role(Config.Role.SHARD):
            return

        if not (config_relation := self.charm.shard.get_config_server_relation()):
            return

        config_relation.data[self.charm.unit][Config.Status.STATUS_READY_FOR_UPGRADE] = json.dumps(
            self.is_unit_status_ready_for_upgrade()
        )

    def is_unit_status_ready_for_upgrade(self) -> bool:
        """Returns True if the status of the current unit reflects that it is ready for upgrade."""
        current_status = self.charm.unit.status
        status_message = current_status.message
        if isinstance(current_status, ActiveStatus):
            return True

        if not isinstance(current_status, WaitingStatus):
            return False

        if status_message and "is not up-to date with config-server" in status_message:
            return True

        return False

    # END: Helpers
