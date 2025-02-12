# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest.mock import Mock, PropertyMock, patch

import httpx
import pytest
from lightkube import ApiError
from ops.model import ActiveStatus, Relation
from ops.testing import ActionFailed, Harness
from parameterized import parameterized
from single_kernel_mongo.config.literals import UnitState
from single_kernel_mongo.core.kubernetes_upgrades import KubernetesUpgrade
from single_kernel_mongo.exceptions import (
    DeployedWithoutTrustError,
    UnhealthyUpgradeError,
)
from tenacity import Future, RetryError

from charm import MongoDBK8sCharm

from .helpers import patch_network_get


@pytest.fixture(autouse=True)
def patch_upgrades(monkeypatch):
    monkeypatch.setattr(
        "single_kernel_mongo.state.charm_state.CharmState.upgrade_in_progress", False
    )
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.get_partition",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.set_partition",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.get_pod",
        lambda *args, **kwargs: 0,
    )


class TestUpgrades(unittest.TestCase):
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self, *unused):
        self.harness = Harness(MongoDBK8sCharm)
        self.addCleanup(self.harness.cleanup)
        mongo_resource = {
            "registrypath": "mongo:4.4",
        }
        self.harness.add_oci_resource("mongodb-image", mongo_resource)
        self.peer_rel_id = self.harness.add_relation("database-peers", "mongodb-peers")
        self.harness.add_relation("upgrade-version-a", "upgrade-version-a")
        self.harness.begin()
        self.harness.set_leader(True)

    @patch("ops.framework.EventBase.defer")
    @patch(
        "single_kernel_mongo.state.charm_state.CharmState.upgrade_in_progress",
        new_callable=PropertyMock,
    )
    def test_on_config_changed_during_upgrade_fails(self, mock_upgrade, defer):
        def is_role_changed_mock(*args):
            return False

        self.harness.charm.operator.state.is_role = is_role_changed_mock

        mock_upgrade.return_value = True
        self.harness.charm.on.config_changed.emit()

        defer.assert_called()

    @parameterized.expand([("relation_joined"), ("relation_changed")])
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    @patch("single_kernel_mongo.managers.config.BackupConfigManager.configure_and_restart")
    @patch("ops.framework.EventBase.defer")
    @patch(
        "single_kernel_mongo.state.charm_state.CharmState.upgrade_in_progress",
        new_callable=PropertyMock,
    )
    def test_on_relation_handler(self, handler, mock_upgrade, defer, *unused):
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        relation: Relation = self.harness.charm.model.get_relation("database-peers")
        mock_upgrade.return_value = True
        getattr(self.harness.charm.on[relation.name], handler).emit(relation)
        defer.assert_called()

    @patch(
        "single_kernel_mongo.state.charm_state.CharmState.upgrade_in_progress",
        new_callable=PropertyMock,
    )
    def test_pass_pre_set_password_check_fails(self, mock_upgrade):
        def mock_shard_role(role_name: str):
            return role_name != "shard"

        mock_pbm_status = Mock(return_value=ActiveStatus())
        self.harness.charm.is_role = mock_shard_role
        mock_upgrade.return_value = True
        self.harness.charm.operator.backup_manager.get_status = mock_pbm_status

        with self.assertRaises(ActionFailed) as action_failed:
            self.harness.run_action("set-password")

        assert (
            action_failed.exception.message
            == "Cannot set passwords while an upgrade is in progress"
        )

    @parameterized.expand([[403, DeployedWithoutTrustError], [500, ApiError]])
    @patch("single_kernel_mongo.managers.k8s.K8sManager.get_partition")
    def test_lightkube_errors(self, status_code, expected_error, patch_get):
        # We need a valid API error due to error handling in lightkube
        api_error = ApiError(
            request=httpx.Request(url="http://controller/call", method="GET"),
            response=httpx.Response(409, json={"message": "bad call", "code": status_code}),
        )
        patch_get.side_effect = api_error

        with self.assertRaises(expected_error):
            KubernetesUpgrade(
                self.harness.charm.operator,
                self.harness.charm.operator.workload,
                self.harness.charm.operator.state,
                self.harness.charm.operator.substrate,
            )

    @parameterized.expand(
        [
            ["6.0.6", "6.0.6", False],
            ["6.0.7", "6.0.6", True],
            ["6.0.6", "6.0.7", True],
        ]
    )
    @patch(
        "single_kernel_mongo.state.charm_state.CharmState.app_workload_container_version",
        new_callable=PropertyMock,
    )
    @patch(
        "single_kernel_mongo.state.charm_state.CharmState.unit_workload_container_version",
        new_callable=PropertyMock,
    )
    def test__get_unit_healthy_status(
        self,
        unit_versions,
        app_version,
        outdated_in_status,
        _unit_version,
        _app_version,
    ) -> None:
        _unit_version.return_value = unit_versions
        _app_version.return_value = app_version

        status = self.harness.charm.operator.upgrade_manager._upgrade._get_unit_healthy_status()
        assert isinstance(status, ActiveStatus)
        assert ("(restart pending)" in status.message) == outdated_in_status

    @parameterized.expand(
        [
            [None, True, "restarting", False],
            [None, True, "restarting", False],
            [None, False, "restarting", True],
            [None, False, "restarting", True],
            [
                RetryError(Future(1)),
                False,
                "restarting",
                True,
            ],
        ]
    )
    @patch("ops.EventBase.defer")
    @patch(
        "single_kernel_mongo.core.abstract_upgrades.GenericMongoDBUpgradeManager.wait_for_cluster_healthy"
    )
    @patch(
        "single_kernel_mongo.core.abstract_upgrades.GenericMongoDBUpgradeManager.is_cluster_able_to_read_write"
    )
    def test_run_post_upgrade_checks(
        self,
        cluster_healthy_return,
        is_cluster_able_to_read_write_return,
        initial_unit_state,
        is_deferred,
        mock_is_cluster,
        mock_wait,
        defer,
    ):
        """Tests the run post upgrade checks branching."""
        mock_wait.side_effect = cluster_healthy_return
        mock_is_cluster.return_value = is_cluster_able_to_read_write_return
        self.harness.charm.operator.state.unit_upgrade_peer_data.unit_state = UnitState(
            initial_unit_state
        )

        if is_deferred:
            with self.assertRaises(UnhealthyUpgradeError):
                self.harness.charm.operator.upgrade_manager.run_post_upgrade_checks(False)
            assert (
                self.harness.charm.operator.state.unit_upgrade_peer_data.unit_state
                == UnitState(initial_unit_state)
            )
        else:
            self.harness.charm.operator.upgrade_manager.run_post_upgrade_checks(False)
            assert (
                self.harness.charm.operator.state.unit_upgrade_peer_data.unit_state
                == UnitState.HEALTHY
            )
