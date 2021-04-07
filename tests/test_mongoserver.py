# Copyright 2021 Canonical Ltd
# See LICENSE file for licensing details.

import unittest

from unittest.mock import patch
from pymongo import MongoClient
from mongoserver import MongoDB

MONGO_CONFIG = {
    'app_name': 'mongodb',
    'replica_set_name': 'rs0',
    'num_peers': 2,
    'port': 27017,
    'root_password': 'password'
}


class TestMongoServer(unittest.TestCase):

    def test_get_client_returns_mongo_client_instance(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        client = mongo.get_client()
        self.assertIsInstance(client, MongoClient)

    def test_get_replica_set_client_returns_mongo_client_instance(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        client = mongo.get_replica_set_client()
        self.assertIsInstance(client, MongoClient)

    @patch('pymongo.MongoClient.server_info')
    def test_mongo_is_ready_when_server_info_is_available(self, server_info):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        server_info.return_value = {"info": "some info"}
        ready = mongo.is_ready()
        self.assertEqual(ready, True)

    @patch('pymongo.MongoClient', 'server_info', 'ServerSelectionTimeoutError')
    def test_mongo_is_not_ready_when_server_info_is_not_available(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        ready = mongo.is_ready()
        self.assertEqual(ready, False)

    @patch('mongoserver.MongoDB.get_replica_set_client')
    @patch('pymongo.MongoClient')
    def test_reconfiguring_replica_invokes_admin_command(self, mock_client, mock_get):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        mock_get.return_value = mock_client

        hosts = {}
        for i in range(config['num_peers']):
            hosts['i'] = "host{}".format(i)

        mongo.reconfigure_replica_set(hosts)
        mock_client.admin.command.assert_called()
        command, _ = mock_client.admin.command.call_args
        self.assertEqual("replSetReconfig", command[0])

    @patch('mongoserver.MongoDB.get_client')
    @patch('pymongo.MongoClient')
    def test_initializing_replica_invokes_admin_command(self, mock_client, mock_get):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        mock_get.return_value = mock_client

        hosts = {}
        for i in range(config['num_peers']):
            hosts['i'] = "host{}".format(i)

        mongo.initialize_replica_set(hosts)
        mock_client.admin.command.assert_called()
        command, _ = mock_client.admin.command.call_args
        self.assertEqual("replSetInitiate", command[0])

    def test_replica_set_uri_contains_correct_number_of_hosts(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)
        uri = mongo.replica_set_uri()
        host_list = uri.split(',')
        self.assertEqual(len(host_list), config['num_peers'])

    def test_replica_set_uri_has_correct_root_credentials(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)
        uri = mongo.replica_set_uri()
        prefix, _ = uri.split('@')
        _, user, password = prefix.split(':')
        user = user.lstrip("/")
        self.assertEqual("root", user)
        self.assertEqual(password, config['root_password'])

    def test_standalone_uri_has_correct_root_credentials(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)
        uri = mongo.standalone_uri()
        prefix, _ = uri.split('@')
        _, user, password = prefix.split(':')
        user = user.lstrip("/")
        self.assertEqual("root", user)
        self.assertEqual(password, config['root_password'])
