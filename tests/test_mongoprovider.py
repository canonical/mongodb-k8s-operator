# Copyright 2021 Canonical Ltd
# See LICENSE file for licensing details.

import json
import unittest

from ops.testing import Harness
from ops.charm import CharmBase
from mongoprovider import MongoProvider

# Used to populate Fake MongoDB metadata.yaml
METADATA = {
    'relation_name': 'database',
    'interface_name': 'mongodb'
}

# Template for Fake MongoDB metadata.yaml
PROVIDER_META = '''
name: fake-mongodb-charm
provides:
  {relation_name}:
    interface: {interface_name}
'''

# Used to populate Fake MongoDB config.yaml
CONFIG = {
    'relation_name': METADATA['relation_name'],
    'is_joined': True,
    'db_version': '4.1.1',
    'replica_set_name': 'rs0',
    'replica_set_uri': 'mongodb://rs0:12701',
    'available_dbs': json.dumps([])
}

# Template for Fake MongoDB charm config.yaml
CONFIG_YAML = '''
options:
  relation_name:
    type: string
    description: 'Fake Relation name used for testing'
    default: {relation_name}
  is_joined:
    type: boolean
    description: 'Does charm have peers'
    default: {is_joined}
  db_version:
    type: string
    description: 'Fake MongoDB version used for testing'
    default: {db_version}
  replica_set_name:
    type: string
    description: 'Name of fake replica set'
    default: {replica_set_name}
  replica_set_uri:
    type: string
    description: 'Fake URI of replicated MongoDB'
    default: {replica_set_uri}
  available_dbs:
    type: string
    description: 'JSON list of availabe databases'
    default: {available_dbs}
'''


class Mongo:
    """Fake mongoserver.MongoDB class"""
    def __init__(self, charm):
        self.charm = charm

    @property
    def databases(self):
        """Return list fake list of available databases"""
        dbs = self.charm.model.config['available_dbs']
        return dbs

    def replica_set_uri(self, credentials=None):
        return self.charm.model.config['replica_set_uri']

    def new_user(self, credentials):
        """Fake MongoDB"""
        pass

    def new_databases(self, credentials, databases):
        """Fake MongoDB does not have to do anything here"""
        pass


class MongoDBCharm(CharmBase):
    """A Fake MongoDB charm used for unit testing MongoProvider"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.mongo = Mongo(self)
        self.provider = MongoProvider(self, 'database')

    @property
    def is_joined(self):
        return True

    @property
    def replica_set_name(self):
        return self.model.config['replica_set_name']

    @property
    def peers(self):
        return self.model.get_relation('mongodb')


class TestMongoProvider(unittest.TestCase):
    def setup_harness(self, config, meta):
        config_yaml = CONFIG_YAML.format(**config)
        meta_yaml = PROVIDER_META.format(**meta)
        self.harness = Harness(MongoDBCharm, meta=meta_yaml, config=config_yaml)
        self.addCleanup(self.harness.cleanup)
        self.harness.set_leader(True)
        self.peer_rel_id = self.harness.add_relation('mongodb', 'mongodb')
        self.harness.begin()

    def test_databases_are_created_when_requested(self):
        config = CONFIG.copy()
        meta = METADATA.copy()
        self.setup_harness(config, meta)

        requested_database = ['mydb']
        json_request = json.dumps(requested_database)
        consumer_data = {'databases': json_request}

        rel_id = self.harness.add_relation('database', 'consumer')
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        self.assertDictEqual(data, {})
        self.harness.add_relation_unit(rel_id, 'consumer/0')
        self.harness.update_relation_data(rel_id, 'consumer', consumer_data)
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        databases = json.loads(data['databases'])
        self.assertListEqual(databases, requested_database)

    def test_databases_are_not_created_without_a_new_request(self):
        config = CONFIG.copy()
        config['available_dbs'] = json.dumps(['mydb1'])
        meta = METADATA.copy()
        self.setup_harness(config, meta)

        requested_database = ['mydb1']
        json_request = json.dumps(requested_database)
        consumer_data = {'databases': json_request}

        rel_id = self.harness.add_relation('database', 'consumer')
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        self.assertDictEqual(data, {})
        self.harness.add_relation_unit(rel_id, 'consumer/0')
        self.harness.update_relation_data(rel_id, 'consumer', consumer_data)
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        self.assertDictEqual(data.get('databases', {}), {})

    def test_databases_are_created_only_by_leader_unit(self):
        config = CONFIG.copy()
        meta = METADATA.copy()
        self.setup_harness(config, meta)
        self.harness.set_leader(False)

        requested_database = ['mydb']
        json_request = json.dumps(requested_database)
        consumer_data = {'databases': json_request}

        rel_id = self.harness.add_relation(meta['relation_name'], 'consumer')
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        self.assertDictEqual(data, {})
        self.harness.add_relation_unit(rel_id, 'consumer/0')
        self.harness.update_relation_data(rel_id, 'consumer', consumer_data)
        data = self.harness.get_relation_data(rel_id, self.harness.model.app.name)
        dbs = data.get('databases', '[]')
        databases = json.loads(dbs)
        self.assertListEqual(databases, [])
