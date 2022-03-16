# Copyright 2021 Canonical Ltd
# See LICENSE file for licensing details.

import unittest

from unittest.mock import patch, Mock, MagicMock
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

    def test_client_returns_mongo_client_instance(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        client = mongo.client()
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

    @patch('mongoserver.MongoDB.client')
    @patch('pymongo.MongoClient')
    def test_reconfiguring_replica_invokes_admin_command(self, mock_client, client):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        client.return_value = mock_client

        hosts = {}
        for i in range(config['num_peers']):
            hosts[i] = "host{}".format(i)

        mongo.reconfigure_replica_set(hosts)
        mock_client.admin.command.assert_called()
        command, _ = mock_client.admin.command.call_args
        self.assertEqual("replSetReconfig", command[0])

    @patch('mongoserver.MongoDB.client')
    @patch('pymongo.MongoClient')
    def test_initializing_replica_invokes_admin_command(self, mock_client, client):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        client.return_value = mock_client

        hosts = {}
        for i in range(config['num_peers']):
            hosts[i] = "host{}".format(i)

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

    def test_replica_set_uri_sets_correct_credentials(self):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        credentials = {"username": "me", "password": "secret"}
        uri = mongo.replica_set_uri(credentials)
        prefix, _ = uri.split('@')
        _, user, password = prefix.split(':')
        user = user.lstrip("/")
        self.assertEqual(credentials["username"], user)
        self.assertEqual(password, credentials["password"])

    @patch('mongoserver.MongoDB.client')
    def test_new_user_requests_user_creation(self, client):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        mock_client = MagicMock()
        client.return_value = mock_client
        mock_db = Mock()
        mock_client.__getitem__.return_value = mock_db

        credentials = {"username": "me", "password": "secret"}
        mongo.new_user(credentials)

        client.assert_called()
        mock_db.command.assert_called_once_with("createUser",
                                                credentials["username"],
                                                pwd=credentials["password"],
                                                roles=[
                                                    {'role': 'userAdminAnyDatabase', 'db': 'admin'}
                                                ])

    @patch('mongoserver.MongoDB.client')
    def test_drop_user_requests_user_removal(self, client):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        mock_client = MagicMock()
        client.return_value = mock_client
        mock_db = Mock()
        mock_client.__getitem__.return_value = mock_db

        username = "me"
        mongo.drop_user(username)

        client.assert_called()
        mock_db.command.assert_called_once_with("dropUser", username)

    @patch('mongoserver.MongoDB.client')
    def test_new_databases_creation_updates_user_privileges(self, client):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        mock_client = MagicMock()
        client.return_value = mock_client
        mock_db = Mock()
        mock_client.__getitem__.return_value = mock_db

        credentials = {"username": "me", "password": "secret"}
        databases = ["mydb"]
        roles = [{"role": "readWrite",
                  "db": databases[0]}]
        mongo.new_databases(credentials, databases)

        client.assert_called()
        mock_db.command.assert_called_once_with("updateUser",
                                                credentials["username"],
                                                roles=roles)

    @patch('mongoserver.MongoDB.client')
    def test_drop_database_requests_database_removal(self, client):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        mock_client = MagicMock()
        client.return_value = mock_client
        mock_db = Mock()
        mock_client.__getitem__.return_value = mock_db

        databases = ["mydb"]
        mongo.drop_databases(databases)
        client.assert_called()
        mock_db.command.assert_called_once_with("dropDatabase")

    @patch('mongoserver.MongoDB.client')
    def test_mongoserver_returns_correct_version(self, client):
        config = MONGO_CONFIG.copy()
        mongo = MongoDB(config)

        version = "1.0.0"
        mock_client = MagicMock()
        client.return_value = mock_client
        mock_client.server_info.return_value = {"version": version}
        self.assertEqual(mongo.version, version)
