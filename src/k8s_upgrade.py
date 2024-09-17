#!/usr/bin/env python3
"""K8S Upgrade code // rough draft only for pre upgrade checks."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from functools import cached_property
from logging import getLogger
from typing import Optional

import lightkube
import lightkube.models.apps_v1
import lightkube.resources.apps_v1
import lightkube.resources.core_v1
from charms.mongodb.v0.upgrade_helpers import (
    PEER_RELATION_ENDPOINT_NAME,
    PRECHECK_ACTION_NAME,
    AbstractUpgrade,
    GenericMongoDBUpgrade,
    PeerRelationNotReady,
    PrecheckFailed,
    unit_number,
)
from lightkube.core.exceptions import ApiError
from ops import ActiveStatus, StatusBase
from ops.charm import ActionEvent, CharmBase
from overrides import override

logger = getLogger()


class DeployedWithoutTrust(Exception):
    """Deployed without `juju deploy --trust` or `juju trust`.

    Needed to access Kubernetes StatefulSet.
    """

    def __init__(self, *, app_name: str):
        super().__init__(
            f"Run `juju trust {app_name} --scope=cluster` and `juju resolve` for each unit (or remove & re-deploy {app_name} with `--trust`)"
        )


class _Partition:
    """StatefulSet partition getter/setter."""

    # Note: I realize this isn't very Pythonic (it'd be nicer to use a property). Because of how
    # ops is structured, we don't have access to the app name when we initialize this class. We
    # need to only initialize this class once so that there is a single cache. Therefore, the app
    # name needs to be passed as argument to the methods (instead of as an argument to __init__)—
    # so we can't use a property.

    def __init__(self):
        # Cache lightkube API call for duration of charm execution
        self._cache: dict[str, int] = {}

    def get(self, *, app_name: str) -> int:
        return self._cache.setdefault(
            app_name,
            lightkube.Client()
            .get(res=lightkube.resources.apps_v1.StatefulSet, name=app_name)
            .spec.updateStrategy.rollingUpdate.partition,
        )

    def set(self, *, app_name: str, value: int) -> None:
        lightkube.Client().patch(
            res=lightkube.resources.apps_v1.StatefulSet,
            name=app_name,
            obj={"spec": {"updateStrategy": {"rollingUpdate": {"partition": value}}}},
        )
        self._cache[app_name] = value


class KubernetesUpgrade(AbstractUpgrade):
    """Code for Kubernetes Upgrade.

    This is a rough draft just to check that pre upgrade checks are still working.
    """

    def __init__(self, charm: CharmBase, *args, **kwargs):
        try:
            partition.get(app_name=charm.app.name)
        except ApiError as err:
            if err.status.code == 403:
                raise DeployedWithoutTrust(app_name=charm.app.name)
            raise
        super().__init__(charm, *args, **kwargs)

    @override
    def _get_unit_healthy_status(self) -> StatusBase:
        version = self._unit_workload_container_versions[self._unit.name]
        if version == self._app_workload_container_version:
            return ActiveStatus(
                f'MongoDB {self._unit_workload_version} running; Img rev {version}; Charmed operator {self._current_versions["charm"]}'
            )
        return ActiveStatus(
            f'MongoDB {self._unit_workload_version} running; Img rev {version} (outdated); Charmed operator {self._current_versions["charm"]}'
        )

    @property
    def _partition(self) -> int:
        """Specifies which units should upgrade.

        Unit numbers >= partition should upgrade
        Unit numbers < partition should not upgrade

        https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#partitions

        For Kubernetes, unit numbers are guaranteed to be sequential
        """
        return partition.get(app_name=self._app_name)

    @_partition.setter
    def _partition(self, value: int) -> None:
        """Sets the partition number."""
        partition.set(app_name=self._app_name, value=value)

    @property
    def upgrade_resumed(self) -> bool:
        """Whether user has resumed upgrade with Juju action."""
        return self._partition < unit_number(self._sorted_units[0])

    @cached_property  # Cache lightkube API call for duration of charm execution
    @override
    def _unit_workload_container_versions(self) -> dict[str, str]:
        """{Unit name: Kubernetes controller revision hash}.

        Even if the workload container version is the same, the workload will restart if the
        controller revision hash changes. (Juju bug: https://bugs.launchpad.net/juju/+bug/2036246).

        Therefore, we must use the revision hash instead of the workload container version. (To
        satisfy the requirement that if and only if this version changes, the workload will
        restart.)
        """
        pods = lightkube.Client().list(
            res=lightkube.resources.core_v1.Pod, labels={"app.kubernetes.io/name": self._app_name}
        )

        def get_unit_name(pod_name: str) -> str:
            *app_name, unit_number = pod_name.split("-")
            return f'{"-".join(app_name)}/{unit_number}'

        return {
            get_unit_name(pod.metadata.name): pod.metadata.labels["controller-revision-hash"]
            for pod in pods
        }

    @cached_property
    @override
    def _app_workload_container_version(self) -> str:
        """Rock revision for current charm code."""
        """App's Kubernetes controller revision hash"""
        stateful_set = lightkube.Client().get(
            res=lightkube.resources.apps_v1.StatefulSet, name=self._app_name
        )
        return stateful_set.status.updateRevision

    @property
    def _unit_workload_version(self) -> Optional[str]:
        """Installed OpenSearch version for this unit."""
        return self._unit_databag.get("workload_version")

    @_unit_workload_version.setter
    def _unit_workload_version(self, value: str):
        self._unit_databag["workload_version"] = value

    def reconcile_partition(self, *, action_event: ActionEvent | None = None) -> None:
        """Handle Juju action to confirm first upgraded unit is healthy and resume upgrade."""
        pass


class MongoDBUpgrade(GenericMongoDBUpgrade):
    """Handlers for upgrade events."""

    def __init__(self, charm: CharmBase):
        self.charm = charm
        super().__init__(charm, PEER_RELATION_ENDPOINT_NAME)

    def _observe_events(self, charm: CharmBase) -> None:
        self.framework.observe(
            charm.on["pre-upgrade-check"].action, self._on_pre_upgrade_check_action
        )

    def _on_pre_upgrade_check_action(self, event: ActionEvent) -> None:
        if not self.charm.unit.is_leader():
            message = f"Must run action on leader unit. (e.g. `juju run {self.charm.app.name}/leader {PRECHECK_ACTION_NAME}`)"
            logger.debug(f"Pre-upgrade check event failed: {message}")
            event.fail(message)
            return
        if not self._upgrade or self._upgrade.in_progress:
            message = "Upgrade already in progress"
            logger.debug(f"Pre-upgrade check event failed: {message}")
            event.fail(message)
            return
        try:
            self._upgrade.pre_upgrade_check()
        except PrecheckFailed as exception:
            message = (
                f"Charm is *not* ready for upgrade. Pre-upgrade check failed: {exception.message}"
            )
            logger.debug(f"Pre-upgrade check event failed: {message}")
            event.fail(message)
            return
        message = "Charm is ready for upgrade"
        event.set_results({"result": message})
        logger.debug(f"Pre-upgrade check event succeeded: {message}")

    @property
    @override
    def _upgrade(self) -> KubernetesUpgrade | None:
        try:
            return KubernetesUpgrade(self.charm)
        except PeerRelationNotReady:
            return None


partition = _Partition()
