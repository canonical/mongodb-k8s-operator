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

    @patch("mongoserver.MongoDB.reconfigure_replica_set")
    def test_replica_set_is_reconfigured_when_peer_joins(self, mock_reconf):
        self.harness.set_leader(True)
        rel_id = self.harness.add_relation('mongodb', 'mongodb')
        self.harness.add_relation_unit(rel_id, 'mongodb/1')
        self.harness.update_relation_data(rel_id,
                                          'mongodb/1',
                                          {'private-address': '10.0.0.1'})
        peers = ['mongodb-0.mongodb-endpoints',
                 'mongodb-1.mongodb-endpoints']
        mock_reconf.assert_called_once_with(peers)

    def test_uri_data_is_generated_correctly(self):
        self.harness.set_leader(True)
        standalone_uri = self.harness.charm.mongo.standalone_uri
        replica_set_uri = self.harness.charm.mongo.replica_set_uri
        self.assertEqual(standalone_uri, 'mongodb://mongodb:27017/')
        self.assertEqual(replica_set_uri, 'mongodb://mongodb-0.mongodb-endpoints:27017/')

    def test_database_relation_data_is_set_correctly(self):
        self.harness.set_leader(True)
        rel_id = self.harness.add_relation('database', 'client')
        self.harness.add_relation_unit(rel_id, 'client/1')
        rel = self.harness.framework.model.get_relation('database', rel_id)
        unit = self.harness.framework.model.get_unit('client/1')
        self.harness.charm.on['database'].relation_changed.emit(rel, unit)
        got = self.harness.get_relation_data(rel_id, self.harness.framework.model.unit.name)
        expected = {
            'replicated': 'False',
            'replica_set_name': 'rs0',
            'standalone_uri': 'mongodb://mongodb:27017/',
            'replica_set_uri': 'mongodb://mongodb-0.mongodb-endpoints:27017/'
        }
        self.assertDictEqual(got, expected)


def replica_set_name(pod_spec):
    containers = pod_spec[0]["containers"]

    for container in containers:
        if container["name"] == "mongodb":
            command = container.get("command")
            idx = command.index("--replSet")
            return command[idx + 1]

    return None
