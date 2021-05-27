# Copyright 2020 Canonical Ltd
# See LICENSE file for licensing details.

import logging
import unittest
from unittest.mock import patch

from ops.testing import Harness
from charm import MongoDBCharm

logger = logging.getLogger(__name__)


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(MongoDBCharm)
        self.addCleanup(self.harness.cleanup)
        mongo_resource = {
            "registrypath": "mongodb:4.4.1",
            "username": "myname",
            "password": "mypassword"
        }
        self.harness.add_oci_resource("mongodb-image", mongo_resource)
        self.harness.begin()

    @patch('ops.testing._TestingPebbleClient.pull')
    def test_replica_set_name_can_be_changed(self, _):
        self.harness.set_leader(True)
        self.harness.container_pebble_ready("mongodb")

        # check default replica set name
        plan = self.harness.get_container_pebble_plan("mongodb")
        self.assertEqual(replica_set_name(plan), "rs0")

        # check replica set name can be changed
        self.harness.update_config({"replica_set_name": "new_name"})
        plan = self.harness.get_container_pebble_plan("mongodb")
        self.assertEqual(replica_set_name(plan), "new_name")

    @patch("mongoserver.MongoDB.reconfigure_replica_set")
    def test_replica_set_is_reconfigured_when_peer_joins(self, mock_reconf):
        self.harness.set_leader(True)
        rel_id = self.harness.add_relation('mongodb', 'mongodb')
        self.harness.add_relation_unit(rel_id, 'mongodb/1')
        self.harness.update_relation_data(rel_id,
                                          'mongodb/1',
                                          {'private-address': '10.0.0.1'})
        peers = ['mongodb-k8s-0.mongodb-k8s-endpoints', 'mongodb-k8s-1.mongodb-k8s-endpoints']
        mock_reconf.assert_called_once_with(peers)

    def test_replica_set_uri_data_is_generated_correctly(self):
        rel_id = self.harness.add_relation('mongodb', 'mongodb')
        self.harness.set_leader(True)
        replica_set_uri = self.harness.charm.mongo.replica_set_uri()
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        cred = "root:{}".format(data['root_password'])
        self.assertEqual(replica_set_uri,
                         'mongodb://{}@mongodb-k8s-0.mongodb-k8s-endpoints:27017/admin'.format(
                             cred))

    def test_leader_sets_key_and_root_credentials(self):
        self.harness.set_leader(False)
        rel_id = self.harness.add_relation('mongodb', 'mongodb')
        self.harness.set_leader(True)
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        self.assertIsNotNone(data['root_password'])
        self.assertIsNotNone(data['security_key'])

    @patch('mongoserver.MongoDB.version')
    def test_charm_provides_version(self, mock_version):
        self.harness.set_leader(True)
        mock_version.return_value = "4.4.1"
        version = self.harness.charm.mongo.version()
        self.assertEqual(version, "4.4.1")

    @patch('mongoserver.MongoDB.is_ready')
    def test_start_is_deferred_if_monog_is_not_ready(self, is_ready):
        is_ready.return_value = False
        self.harness.set_leader(True)
        with self.assertLogs(level="DEBUG") as logger:
            self.harness.charm.on.start.emit()
            is_ready.assert_called()
            for message in sorted(logger.output):
                if "DEBUG:ops.framework:Deferring" in message:
                    self.assertIn("StartEvent", message)

    @patch('mongoserver.MongoDB.initialize_replica_set')
    @patch('mongoserver.MongoDB.is_ready')
    def test_start_is_deffered_if_monog_is_not_initialized(self, is_ready, initialize):
        is_ready.return_value = True
        initialize.side_effect = RuntimeError("Not Initialized")
        self.harness.set_leader(True)
        with self.assertLogs(level="DEBUG") as logger:
            self.harness.charm.on.start.emit()
            is_ready.assert_called()
            self.assertIn("INFO:charm:Deferring on_start since : error=Not Initialized",
                          sorted(logger.output))


def replica_set_name(plan, service="mongodb"):
    plan_dict = plan.to_dict()
    command = plan_dict["services"][service]["command"]
    args = command.split()
    idx = args.index("--replSet")
    return args[idx + 1]

    return None
