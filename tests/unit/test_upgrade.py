# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest.mock import Mock, PropertyMock, patch

from ops.model import ActiveStatus, Relation
from ops.testing import ActionFailed, Harness
from parameterized import parameterized

from charm import MongoDBCharm
from config import Config

from .helpers import patch_network_get


class TestUpgrades(unittest.TestCase):
    @patch_network_get(private_address="1.1.1.1")
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
    def test_on_relation_handler(self, handler, mock_upgrade, defer, *unused):
        relation: Relation = self.harness.charm.model.get_relation("database-peers")
        mock_upgrade.return_value = True
        getattr(self.harness.charm.on[Config.Relations.PEERS], handler).emit(relation)
        defer.assert_called()

    @patch("charm.MongoDBCharm.upgrade_in_progress", new_callable=PropertyMock)
    def test_pass_pre_set_password_check_fails(self, mock_upgrade):
        def mock_shard_role(*args):
            return args != ("shard",)

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
