#!/usr/bin/env python3
"""Kubernetes Upgrade Code.

This code is slightly different from the code which was written originally.
It is required to deploy the application with `--trust` for this code to work
as it has to interact with the Kubernetes StatefulSet.
The main differences are:
 * Add the handling of workload version + version sharing on the cluster in the
 upgrade handler + relation created handler.
 * Add the two post upgrade events that check the cluster health and run it if
 we are in state `RESTARTING`.
"""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from logging import getLogger
from typing import TYPE_CHECKING

from charms.mongodb.v0.upgrade_helpers import (
    PEER_RELATION_ENDPOINT_NAME,
    PRECHECK_ACTION_NAME,
    RESUME_ACTION_NAME,
    ROLLBACK_INSTRUCTIONS,
    GenericMongoDBUpgrade,
    PeerRelationNotReady,
    PrecheckFailed,
    UnitState,
)
from charms.mongodb.v1.mongos import BalancerNotEnabledError, MongosConnection
from ops import ActiveStatus
from ops.charm import ActionEvent
from ops.framework import EventBase, EventSource
from ops.model import BlockedStatus
from overrides import override
from tenacity import RetryError

from config import Config
from upgrades.kubernetes_upgrades import KubernetesUpgrade

if TYPE_CHECKING:
    from charm import MongoDBCharm

logger = getLogger()


class _PostUpgradeCheckMongoDB(EventBase):
    """Run post upgrade check on MongoDB to verify that the cluster is healhty."""

    def __init__(self, handle):
        super().__init__(handle)


class MongoDBUpgrade(GenericMongoDBUpgrade):
    """Handlers for upgrade events."""

    post_app_upgrade_event = EventSource(_PostUpgradeCheckMongoDB)
    post_cluster_upgrade_event = EventSource(_PostUpgradeCheckMongoDB)

    def __init__(self, charm: "MongoDBCharm"):
        self.charm = charm
        super().__init__(charm, PEER_RELATION_ENDPOINT_NAME)

    @override
    def _observe_events(self, charm: "MongoDBCharm") -> None:
        self.framework.observe(
            charm.on[PRECHECK_ACTION_NAME].action, self._on_pre_upgrade_check_action
        )
        self.framework.observe(
            charm.on[PEER_RELATION_ENDPOINT_NAME].relation_created,
            self._on_upgrade_peer_relation_created,
        )
        self.framework.observe(
            charm.on[PEER_RELATION_ENDPOINT_NAME].relation_changed, self._reconcile_upgrade
        )
        self.framework.observe(charm.on[RESUME_ACTION_NAME].action, self._on_resume_upgrade_action)
        self.framework.observe(self.post_app_upgrade_event, self.run_post_app_upgrade_task)
        self.framework.observe(self.post_cluster_upgrade_event, self.run_post_cluster_upgrade_task)

    def _reconcile_upgrade(self, _, during_upgrade: bool = False) -> None:
        """Handle upgrade events."""
        if (
            self.charm.get_termination_period_for_pod()
            != Config.WebhookManager.GRACE_PERIOD_SECONDS
        ) or self.charm.first_time_with_new_termination_period:
            logger.debug("Pod hasn't reastarted at least once")
            return

        if self.charm.needs_new_termination_period:
            logger.debug("Can't upgrade before graceTerminationPeriod is set")
            return
        if not self._upgrade:
            logger.debug("Peer relation not available")
            return
        if not self._upgrade.versions_set:
            logger.debug("Peer relation not ready")
            return
        if self.charm.unit.is_leader() and not self._upgrade.in_progress:
            # Run before checking `self._upgrade.is_compatible` in case incompatible upgrade was
            # forced & completed on all units.
            self.charm.version_checker.set_version_across_all_relations()
            self._upgrade.set_versions_in_app_databag()

        if self._upgrade.unit_state is UnitState.RESTARTING:  # Kubernetes only
            if not self._upgrade.is_compatible:
                logger.info(
                    f"Refresh incompatible. If you accept potential *data loss* and *downtime*, you can continue with `{RESUME_ACTION_NAME} force=true`"
                )
                self.charm.status.set_and_share_status(Config.Status.INCOMPATIBLE_UPGRADE)
                return
        if not during_upgrade and self.charm.db_initialised and self.charm.is_db_service_ready():
            self._upgrade.unit_state = UnitState.HEALTHY
            self.charm.status.set_and_share_status(ActiveStatus())
        if self.charm.unit.is_leader():
            self._upgrade.reconcile_partition()

        self._set_upgrade_status()

    def _set_upgrade_status(self):
        if self.charm.unit.is_leader():
            self.charm.app.status = self._upgrade.app_status or ActiveStatus()
        # Set/clear upgrade unit status if no other unit status - upgrade status for units should
        # have the lowest priority.
        if (
            isinstance(self.charm.unit.status, ActiveStatus)
            or (
                isinstance(self.charm.unit.status, BlockedStatus)
                and self.charm.unit.status.message.startswith(
                    "Rollback with `juju refresh`. Pre-refresh check failed:"
                )
            )
            or self.charm.unit.status == Config.Status.WAITING_POST_UPGRADE_STATUS
        ):
            self.charm.status.set_and_share_status(
                self._upgrade.get_unit_juju_status() or ActiveStatus()
            )

    def _on_upgrade_peer_relation_created(self, _) -> None:
        """First time the relation is created, we save the revisions."""
        if self.charm.unit.is_leader():
            self._upgrade.set_versions_in_app_databag()

    def _on_resume_upgrade_action(self, event: ActionEvent) -> None:
        if not self.charm.unit.is_leader():
            message = f"Must run action on leader unit. (e.g. `juju run {self.charm.app.name}/leader {RESUME_ACTION_NAME}`)"
            logger.debug(f"Resume refresh failed: {message}")
            event.fail(message)
            return
        if not self._upgrade or not self._upgrade.in_progress:
            message = "No upgrade in progress"
            logger.debug(f"Resume refresh failed: {message}")
            event.fail(message)
            return
        self._upgrade.reconcile_partition(action_event=event)

    def run_post_app_upgrade_task(self, event: EventBase):
        """Runs the post upgrade check to verify that the cluster is healthy.

        By deferring before setting unit state to HEALTHY, the user will either:
            1. have to wait for the unit to resolve itself.
            2. have to run the force-upgrade action (to upgrade the next unit).
        """
        logger.debug(
            "Running post refresh checks to verify the deployment is not broken after refresh."
        )
        self.run_post_upgrade_checks(event, finished_whole_cluster=False)

        if self._upgrade.unit_state != UnitState.HEALTHY:
            logger.info(
                f"Unit state is not healhy but {self._upgrade.unit_state}, not continuing post-refresh checks."
            )
            return

        # Leader of config-server must wait for all shards to be upgraded before finalising the
        # upgrade.
        if not self.charm.unit.is_leader() or not self.charm.is_role(Config.Role.CONFIG_SERVER):
            logger.debug("Post refresh check is completed.")
            return

        self.charm.upgrade.post_cluster_upgrade_event.emit()

    def run_post_cluster_upgrade_task(self, event: EventBase) -> None:
        """Waits for entire cluster to be upgraded before enabling the balancer."""
        # Leader of config-server must wait for all shards to be upgraded before finalising the
        # upgrade.
        if not self.charm.unit.is_leader() or not self.charm.is_role(Config.Role.CONFIG_SERVER):
            return

        if not self.charm.is_cluster_on_same_revision():
            logger.debug("Waiting to finalise refresh, one or more shards need refresh.")
            event.defer()
            return

        logger.debug(
            "Entire cluster has been refreshed, checking health of the cluster and enabling balancer."
        )
        self.run_post_upgrade_checks(event, finished_whole_cluster=True)

        try:
            with MongosConnection(self.charm.mongos_config) as mongos:
                mongos.start_and_wait_for_balancer()
        except BalancerNotEnabledError:
            logger.debug(
                "Need more time to enable the balancer after finishing the refresh. Deferring event."
            )
            event.defer()
            return

        self.set_mongos_feature_compatibilty_version(Config.Upgrade.FEATURE_VERSION_6)

    def _on_pre_upgrade_check_action(self, event: ActionEvent) -> None:
        """Runs the pre-refresh checks to ensure that the deployment is ready for refresh."""
        if not self.charm.unit.is_leader():
            message = f"Must run action on leader unit. (e.g. `juju run {self.charm.app.name}/leader {PRECHECK_ACTION_NAME}`)"
            logger.debug(f"Pre-refresh check failed: {message}")
            event.fail(message)
            return
        if not self._upgrade or self._upgrade.in_progress:
            message = "Upgrade already in progress"
            logger.debug(f"Pre-refresh check failed: {message}")
            event.fail(message)
            return
        try:
            self._upgrade.pre_upgrade_check()
        except PrecheckFailed as exception:
            message = (
                f"Charm is *not* ready for refresh. Pre-refresh check failed: {exception.message}"
            )
            logger.debug(f"Pre-refresh check failed: {message}")
            event.fail(message)
            return
        message = "Charm is ready for upgrade"
        event.set_results({"result": message})
        logger.debug(f"Pre-refresh check succeeded: {message}")

    @property
    @override
    def _upgrade(self) -> KubernetesUpgrade | None:
        try:
            return KubernetesUpgrade(self.charm)
        except PeerRelationNotReady:
            return None

    def run_post_upgrade_checks(self, event, finished_whole_cluster: bool) -> None:
        """Runs post-upgrade checks for after a shard/config-server/replset/cluster upgrade."""
        upgrade_type = "unit" if not finished_whole_cluster else "sharded cluster"
        if not self.charm.db_initialised:
            self._upgrade.unit_state = UnitState.HEALTHY
            return
        try:
            self.wait_for_cluster_healthy()
        except RetryError:
            logger.error(
                "Cluster is not healthy after refreshing %s. Will retry next juju event.",
                upgrade_type,
            )
            logger.info(ROLLBACK_INSTRUCTIONS)
            self.charm.status.set_and_share_status(Config.Status.UNHEALTHY_UPGRADE)
            event.defer()
            return

        if not self.is_cluster_able_to_read_write():
            logger.error(
                "Cluster is not healthy after refreshing %s, writes not propagated throughout cluster. Deferring post refresh check.",
                upgrade_type,
            )
            logger.info(ROLLBACK_INSTRUCTIONS)
            self.charm.status.set_and_share_status(Config.Status.UNHEALTHY_UPGRADE)
            event.defer()
            return

        if self.charm.unit.status == Config.Status.UNHEALTHY_UPGRADE:
            self.charm.status.set_and_share_status(ActiveStatus())

        self._upgrade.unit_state = UnitState.HEALTHY
