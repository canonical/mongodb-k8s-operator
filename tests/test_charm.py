# Copyright 2020 Canonical Ltd
# See LICENSE file for licensing details.

import unittest
from unittest.mock import patch

from ops.testing import Harness
from charm import MongoDBCharm


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

    def test_replica_set_name_can_be_changed(self):
        self.harness.set_leader(True)

        # check default replica set name
        self.harness.charm.on.config_changed.emit()
        pod_spec = self.harness.get_pod_spec()
        self.assertEqual(replica_set_name(pod_spec), "rs0")

        # check replica set name can be changed
        self.harness.update_config({"replica_set_name": "new_name"})
        pod_spec = self.harness.get_pod_spec()
        self.assertEqual(replica_set_name(pod_spec), "new_name")

    @patch("mongo.Mongo.reconfigure_replica_set")
    def test_replica_set_is_reconfigured_when_peer_joins(self, mock_reconf):
        self.harness.set_leader(True)
        rel_id = self.harness.add_relation('mongodb', 'mongodb')
        self.harness.add_relation_unit(rel_id, 'mongodb/1')
        self.harness.update_relation_data(rel_id,
                                          'mongodb/1',
                                          {'private-address': '10.0.0.1'})
        self.assertEqual(self.harness.charm.num_peers, 2)
        peers = ['mongodb-0.mongodb-endpoints',
                 'mongodb-1.mongodb-endpoints']
        mock_reconf.assert_called_once_with(peers)


def replica_set_name(pod_spec):
    containers = pod_spec[0]["containers"]

    for container in containers:
        if container["name"] == "mongodb":
            command = container.get("command")
            idx = command.index("--replSet")
            return command[idx + 1]

    return None
