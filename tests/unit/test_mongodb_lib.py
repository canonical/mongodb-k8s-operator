# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import unittest
from unittest.mock import call, patch

from charms.mongodb.v0.mongodb import MongoDBConnection, NotReadyError
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure

MONGO_CONFIG = {
    "replset": "mongo-k8s",
    "database": "admin",
    "username": "operator",
    "password": "password",
    "hosts": set(["1.1.1.1", "2.2.2.2"]),
}

PYMONGO_EXCEPTIONS = [
    (ConnectionFailure("error message"), ConnectionFailure),
    (ConfigurationError("error message"), ConfigurationError),
    (OperationFailure("error message"), OperationFailure),
]


class TestMongoServer(unittest.TestCase):
    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_is_ready_error_handling(self, config, mock_client):
        """Test failure to check ready of replica returns False.

        Test also verifies that when an exception is raised we still close the client connection.
        """
        for exception, _ in PYMONGO_EXCEPTIONS:
            with MongoDBConnection(config) as mongo:
                mock_client.return_value.admin.command.side_effect = exception

                #  verify ready is false when an error occurs
                ready = mongo.is_ready
                self.assertEqual(ready, False)

            # verify we close connection
            (mock_client.return_value.close).assert_called()

    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_init_replset_error_handling(self, config, mock_client):
        """Test failure to initialise replica set raises an error.

        Test also verifies that when an exception is raised we still close the client connection.
        """
        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            with self.assertRaises(expected_raise):
                with MongoDBConnection(config) as mongo:
                    mock_client.return_value.admin.command.side_effect = exception
                    mongo.init_replset()

            # verify we close connection
            (mock_client.return_value.close).assert_called()

    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_get_replset_members_error_handling(self, config, mock_client):
        """Test failure to get replica set members raises an error.

        Test also verifies that when an exception is raised we still close the client connection.
        """
        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            with self.assertRaises(expected_raise):
                with MongoDBConnection(config) as mongo:
                    mock_client.return_value.admin.command.side_effect = exception
                    mongo.get_replset_members()

            # verify we close connection
            (mock_client.return_value.close).assert_called()

    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_add_replset_members_pymongo_error_handling(self, config, mock_client):
        """Test failures related to PyMongo properly get handled in add_replset_member.

        Test also verifies that when an exception is raised we still close the client connection
        and that no attempt to replSetReconfig is made.
        """
        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            with self.assertRaises(expected_raise):
                with MongoDBConnection(config) as mongo:
                    mock_client.return_value.admin.command.side_effect = exception
                    mongo.add_replset_member("hostname")

            # verify we close connection
            (mock_client.return_value.close).assert_called()

    @patch("charms.mongodb.v0.mongodb.MongoDBConnection._is_any_sync")
    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_add_replset_member_wait_to_sync(self, config, mock_client, any_sync):
        """Tests that adding replica set members raises NotReadyError if another member is syncing.

        Test also verifies that when an exception is raised we still close the client connection
        and that no attempt to replSetReconfig is made.
        """
        any_sync.return_value = True
        with self.assertRaises(NotReadyError):
            with MongoDBConnection(config) as mongo:
                mongo.add_replset_member("hostname")

        # verify we close connection and that no attempt to reconfigure was made
        (mock_client.return_value.close).assert_called()

        actual_calls = mock_client.return_value.admin.command.mock_calls
        no_reconfig = call("replSetReconfig") not in actual_calls
        self.assertEqual(no_reconfig, True)

    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_remove_replset_members_pymongo_error_handling(self, config, mock_client):
        """Test failures related to PyMongo properly get handled in remove_replset_member.

        Test also verifies that when an exception is raised we still close the client connection
        and that no attempt to replSetReconfig is made.
        """
        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            with self.assertRaises(expected_raise):
                with MongoDBConnection(config) as mongo:
                    mock_client.return_value.admin.command.side_effect = exception
                    mongo.remove_replset_member("hostname")

            # verify we close connection
            (mock_client.return_value.close).assert_called()

    @patch("charms.mongodb.v0.mongodb.MongoDBConnection._is_any_removing")
    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_remove_replset_member_wait_to_remove(self, config, mock_client, any_remove):
        """Tests removing replica set members raises NotReadyError if another member is removing.

        Test also verifies that when an exception is raised we still close the client connection
        and that no attempt to replSetReconfig is made.
        """
        any_remove.return_value = True
        with self.assertRaises(NotReadyError):
            with MongoDBConnection(config) as mongo:
                mongo.remove_replset_member("hostname")

        # verify we close connection and that no attempt to reconfigure was made
        (mock_client.return_value.close).assert_called()

        actual_calls = mock_client.return_value.admin.command.mock_calls
        no_reconfig = call("replSetReconfig") not in actual_calls
        self.assertEqual(no_reconfig, True)

    @patch("charms.mongodb.v0.mongodb.MongoDBConnection._is_any_removing")
    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_create_user_error_handling(self, config, mock_client, any_remove):
        """Test failures related to PyMongo properly get handled when creating a user.

        Test also verifies that when an exception is raised we still close the client connection.
        """
        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            with self.assertRaises(expected_raise):
                with MongoDBConnection(config) as mongo:
                    mock_client.return_value.admin.command.side_effect = exception
                    mongo.create_user(config)

            # verify we close connection
            (mock_client.return_value.close).assert_called()

    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_update_user_error_handling(self, config, mock_client):
        """Test failures related to PyMongo properly get handled when updating a user.

        Test also verifies that when an exception is raised we still close the client connection.
        """
        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            with self.assertRaises(expected_raise):
                with MongoDBConnection(config) as mongo:
                    mock_client.return_value.admin.command.side_effect = exception
                    mongo.update_user(config)

            # verify we close connection
            (mock_client.return_value.close).assert_called()

    @patch("charms.mongodb.v0.mongodb.MongoClient")
    @patch("charms.mongodb.v0.mongodb.MongoDBConfiguration")
    def test_drop_user_error_handling(self, config, mock_client):
        """Test failures related to PyMongo properly get handled when dropping a user.

        Test also verifies that when an exception is raised we still close the client connection.
        """
        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            with self.assertRaises(expected_raise):
                with MongoDBConnection(config) as mongo:
                    mock_client.return_value.admin.command.side_effect = exception
                    mongo.drop_user("username")

            # verify we close connection
            (mock_client.return_value.close).assert_called()
