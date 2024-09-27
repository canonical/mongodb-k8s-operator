# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest.mock import Mock, PropertyMock, patch

import httpx
import pytest
from charms.mongodb.v0.upgrade_helpers import UnitState
from lightkube import ApiError
from ops import StartEvent
from ops.model import ActiveStatus, Relation
from ops.testing import ActionFailed, Harness
from parameterized import parameterized
from tenacity import Future, RetryError

from charm import MongoDBCharm
from config import Config
from k8s_upgrade import DeployedWithoutTrust, KubernetesUpgrade

from .helpers import patch_network_get


@pytest.fixture(autouse=True)
def patch_upgrades(monkeypatch):
    monkeypatch.setattr("charms.mongodb.v0.upgrade_helpers.AbstractUpgrade.in_progress", False)
    monkeypatch.setattr("charm.k8s_upgrade._Partition.get", lambda *args, **kwargs: 0)
    monkeypatch.setattr("charm.k8s_upgrade._Partition.set", lambda *args, **kwargs: None)


class TestUpgrades(unittest.TestCase):
    @patch("charm.get_charm_revision")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.get_charm_revision")
    def setUp(self, *unused):
        self.harness = Harness(MongoDBCharm)
        self.addCleanup(self.harness.cleanup)
        mongo_resource = {
            "registrypath": "mongo:4.4",
        }
        self.harness.add_oci_resource("mongodb-image", mongo_resource)
        self.harness.begin()
        self.harness.set_leader(True)
        self.peer_rel_id = self.harness.add_relation("database-peers", "mongodb-peers")
        self.harness.add_relation("upgrade-version-a", "upgrade-version-a")

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm.upgrade_in_progress", new_callable=PropertyMock)
    def test_on_config_changed_during_upgrade_fails(self, mock_upgrade, defer):
        def is_role_changed_mock(*args):
            return True

        self.harness.charm.is_role_changed = is_role_changed_mock

        mock_upgrade.return_value = True
        self.harness.charm.on.config_changed.emit()

        defer.assert_called()

    @parameterized.expand([("relation_joined"), ("relation_changed")])
    @patch("charm.MongoDBCharm._connect_pbm_agent")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm.upgrade_in_progress", new_callable=PropertyMock)
    @patch("charm.MongoDBCharm.is_db_service_ready")
    def test_on_relation_handler(self, handler, mock_upgrade, defer, *unused):
        relation: Relation = self.harness.charm.model.get_relation("database-peers")
        mock_upgrade.return_value = True
        getattr(self.harness.charm.on[Config.Relations.PEERS], handler).emit(relation)
        defer.assert_called()

    @patch("charm.MongoDBCharm.upgrade_in_progress", new_callable=PropertyMock)
    def test_pass_pre_set_password_check_fails(self, mock_upgrade):
        def mock_shard_role(role_name: str):
            return role_name != "shard"

        mock_pbm_status = Mock(return_value=ActiveStatus())
        self.harness.charm.is_role = mock_shard_role
        mock_upgrade.return_value = True
        self.harness.charm.backups.get_pbm_status = mock_pbm_status

        with self.assertRaises(ActionFailed) as action_failed:
            self.harness.run_action("set-password")

        assert (
            action_failed.exception.message
            == "Cannot set passwords while an upgrade is in progress."
        )

    @parameterized.expand([[403, DeployedWithoutTrust], [500, ApiError]])
    @patch("charm.k8s_upgrade._Partition.get")
    def test_lightkube_errors(self, status_code, expected_error, patch_get):
        # We need a valid API error due to error handling in lightkube
        api_error = ApiError(
            request=httpx.Request(url="http://controller/call", method="GET"),
            response=httpx.Response(409, json={"message": "bad call", "code": status_code}),
        )
        patch_get.side_effect = api_error

        with self.assertRaises(expected_error):
            KubernetesUpgrade(self.harness.charm)

    @parameterized.expand(
        [
            [{"mongodb-k8s/0": "6.0.6"}, "6.0.6", False],
            [{"mongodb-k8s/0": "6.0.7"}, "6.0.6", True],
            [{"mongodb-k8s/0": "6.0.6"}, "6.0.7", True],
        ]
    )
    @patch(
        "charm.k8s_upgrade.KubernetesUpgrade._app_workload_container_version",
        new_callable=PropertyMock,
    )
    @patch(
        "charm.k8s_upgrade.KubernetesUpgrade._unit_workload_container_versions",
        new_callable=PropertyMock,
    )
    def test__get_unit_healthy_status(
        self, unit_versions, app_version, outdated_in_status, _unit_version, _app_version
    ) -> None:
        _unit_version.return_value = unit_versions
        _app_version.return_value = app_version

        status = self.harness.charm.upgrade._upgrade._get_unit_healthy_status()
        assert isinstance(status, ActiveStatus)
        assert ("(restart pending)" in status.message) == outdated_in_status

    @parameterized.expand(
        [
            [None, True, ActiveStatus(), "restarting", False],
            [None, True, Config.Status.UNHEALTHY_UPGRADE, "restarting", False],
            [None, False, ActiveStatus(), "restarting", True],
            [None, False, Config.Status.UNHEALTHY_UPGRADE, "restarting", True],
            [RetryError(Future(1)), False, Config.Status.UNHEALTHY_UPGRADE, "restarting", True],
        ]
    )
    @patch("ops.EventBase.defer")
    @patch("charm.k8s_upgrade.MongoDBUpgrade.wait_for_cluster_healthy")
    @patch("charm.k8s_upgrade.MongoDBUpgrade.is_cluster_able_to_read_write")
    def test_run_post_upgrade_checks(
        self,
        cluster_healthy_return,
        is_cluster_able_to_read_write_return,
        initial_status,
        initial_unit_state,
        is_deferred,
        mock_is_cluster,
        mock_wait,
        defer,
    ):
        """Tests the run post upgrade checks branching."""
        mock_wait.side_effect = cluster_healthy_return
        mock_is_cluster.return_value = is_cluster_able_to_read_write_return
        self.harness.charm.unit.status = initial_status
        self.harness.charm.upgrade._upgrade.unit_state = UnitState(initial_unit_state)

        self.harness.charm.upgrade.run_post_upgrade_checks(StartEvent, False)
        if is_deferred:
            defer.assert_called()
            assert self.harness.charm.unit.status == Config.Status.UNHEALTHY_UPGRADE
            assert self.harness.charm.upgrade._upgrade.unit_state == UnitState(initial_unit_state)
        else:
            assert self.harness.charm.unit.status == ActiveStatus()
            assert self.harness.charm.upgrade._upgrade.unit_state == UnitState.HEALTHY
