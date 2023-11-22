# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import re
import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from charms.mongodb.v0.helpers import CONF_DIR, DATA_DIR, KEY_FILE
from ops.model import ActiveStatus, MaintenanceStatus, ModelError
from ops.pebble import APIError, ExecError, PathError, ProtocolError
from ops.testing import Harness
from parameterized import parameterized
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    OperationFailure,
    PyMongoError,
)

from charm import MongoDBCharm, NotReadyError

from .helpers import patch_network_get

PYMONGO_EXCEPTIONS = [
    (ConnectionFailure("error message"), ConnectionFailure),
    (ConfigurationError("error message"), ConfigurationError),
    (OperationFailure("error message"), OperationFailure),
]
PEER_ADDR = {"private-address": "127.4.5.6"}

logger = logging.getLogger(__name__)


class TestCharm(unittest.TestCase):
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self):
        self.maxDiff = None
        self.harness = Harness(MongoDBCharm)
        mongo_resource = {
            "registrypath": "mongo:4.4",
        }
        self.harness.add_oci_resource("mongodb-image", mongo_resource)
        self.harness.begin()
        self.harness.add_relation("database-peers", "mongodb-peers")
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    @pytest.fixture
    def use_caplog(self, caplog):
        self._caplog = caplog

    def _setup_secrets(self):
        self.harness.set_leader(True)
        self.harness.charm._generate_secrets()
        self.harness.set_leader(False)

    @patch("charm.MongoDBCharm._pull_licenses")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm._set_data_dir_permissions")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    def test_mongod_pebble_ready(self, connect_exporter, fix_data_dir, defer, pull_licenses):
        # Expected plan after Pebble ready with default config
        expected_plan = {
            "services": {
                "mongod": {
                    "user": "mongodb",
                    "group": "mongodb",
                    "override": "replace",
                    "summary": "mongod",
                    "command": (
                        "mongod --bind_ip_all "
                        "--replSet=mongodb-k8s "
                        f"--dbpath={DATA_DIR} "
                        "--logpath=/var/lib/mongodb/mongodb.log --auth "
                        "--clusterAuthMode=keyFile "
                        f"--keyFile={CONF_DIR}/{KEY_FILE} \n"
                    ),
                    "startup": "enabled",
                }
            },
        }
        # Get the mongod container from the model
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        # Emit the PebbleReadyEvent carrying the mongod container
        self.harness.charm.on.mongod_pebble_ready.emit(container)
        # Get the plan now we've run PebbleReady
        updated_plan = self.harness.get_container_pebble_plan("mongod").to_dict()
        # Check we've got the plan we expected
        assert expected_plan == updated_plan
        # Check the service was started
        service = self.harness.model.unit.get_container("mongod").get_service("mongod")
        assert service.is_running()
        defer.assert_not_called()
        # Ensure that _connect_mongodb_exporter was called
        connect_exporter.assert_called_once()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm._push_keyfile_to_workload")
    def test_pebble_ready_cannot_retrieve_container(self, push_keyfile_to_workload, defer):
        """Test verifies behavior when retrieving container results in ModelError in pebble ready.

        Verifies that when a failure to get a container occurs, that that failure is raised and
        that no efforts to set keyFile or add/replan layers are made.
        """
        # presets
        self.harness.set_leader(True)
        mock_container = mock.Mock()
        mock_container.side_effect = ModelError
        self.harness.charm.unit.get_container = mock_container

        with self.assertRaises(ModelError):
            self.harness.charm.on.mongod_pebble_ready.emit(mock_container)

        push_keyfile_to_workload.assert_not_called()
        mock_container.add_layer.assert_not_called()
        mock_container.replan.assert_not_called()
        defer.assert_not_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm._push_keyfile_to_workload")
    def test_pebble_ready_container_cannot_connect(self, push_keyfile_to_workload, defer):
        """Test verifies behavior when cannot connect to container in pebble ready function.

        Verifies that when a failure to connect to container results in a deferral and that no
        efforts to set keyFile or add/replan layers are made.
        """
        # presets
        self.harness.set_leader(True)
        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = False
        self.harness.charm.unit.get_container = mock_container

        # Emit the PebbleReadyEvent carrying the mongod container
        self.harness.charm.on.mongod_pebble_ready.emit(mock_container)

        push_keyfile_to_workload.assert_not_called()
        mock_container.add_layer.assert_not_called()
        mock_container.replan.assert_not_called()
        defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm._push_keyfile_to_workload")
    def test_pebble_ready_push_keyfile_to_workload_failure(self, push_keyfile_to_workload, defer):
        """Test verifies behavior when setting keyfile fails.

        Verifies that when a failure to set keyfile occurs that there is no attempt to add layers
        or replan the container.
        """
        # presets
        self.harness.set_leader(True)
        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        self.harness.charm.unit.get_container = mock_container

        for exception in [PathError("kind", "message"), ProtocolError("kind", "message")]:
            push_keyfile_to_workload.side_effect = exception

            # Emit the PebbleReadyEvent carrying the mongod container
            self.harness.charm.on.mongod_pebble_ready.emit(mock_container)
            mock_container.add_layer.assert_not_called()
            mock_container.replan.assert_not_called()
            defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_cannot_retrieve_container(self, connection, init_user, provider, defer):
        """Verifies that failures to get container result in a ModelError being raised.

        Further this function verifies that on error no attempts to set up the replica set or
        database users are made.
        """
        # presets
        self.harness.set_leader(True)
        mock_container = mock.Mock()
        mock_container.side_effect = ModelError
        self.harness.charm.unit.get_container = mock_container
        with self.assertRaises(ModelError):
            self.harness.charm.on.start.emit()

        # when cannot retrieve a container we should not set up the replica set or handle users
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_user.assert_not_called()
        provider.return_value.oversee_users.assert_not_called()

        # verify app data
        self.assertEqual("db_initialised" in self.harness.charm.app_peer_data, False)
        defer.assert_not_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_container_cannot_connect(self, connection, init_user, provider, defer):
        """Tests inability to connect results in deferral.

        Verifies that if connection is not possible, that there are no attempts to set up the
        replica set or handle users.
        """
        # presets
        self.harness.set_leader(True)
        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = False
        self.harness.charm.unit.get_container = mock_container

        self.harness.charm.on.start.emit()

        # when cannot connect to container we should not set up the replica set or handle users
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_user.assert_not_called()
        provider.return_value.oversee_users.assert_not_called()

        # verify app data
        self.assertEqual("db_initialised" in self.harness.charm.app_peer_data, False)
        defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_container_does_not_exist(self, connection, init_user, provider, defer):
        """Tests lack of existence of files on container results in deferral.

        Verifies that if files do not exists, that there are no attempts to set up the replica set
        or handle users.
        """
        # presets
        self.harness.set_leader(True)
        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.return_value = False
        self.harness.charm.unit.get_container = mock_container

        self.harness.charm.on.start.emit()

        # when container does not exist we should not set up the replica set or handle users
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_user.assert_not_called()
        provider.return_value.oversee_users.assert_not_called()

        # verify app data
        self.assertEqual("db_initialised" in self.harness.charm.app_peer_data, False)
        defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_container_exists_fails(self, connection, init_user, provider, defer):
        """Tests failure in checking file existence on container raises an APIError.

        Verifies that when checking container files raises an API Error, we raise that same error
        and make no attempts to set up the replica set or handle users.
        """
        # presets
        self.harness.set_leader(True)
        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.side_effect = APIError("body", 0, "status", "message")
        self.harness.charm.unit.get_container = mock_container

        with self.assertRaises(APIError):
            self.harness.charm.on.start.emit()

        # when container does not exist we should not set up the replica set or handle users
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_user.assert_not_called()
        provider.return_value.oversee_users.assert_not_called()

        # verify app data
        self.assertEqual("db_initialised" in self.harness.charm.app_peer_data, False)
        defer.assert_not_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_already_initialised(self, connection, init_user, provider, defer):
        """Tests that if the replica set has already been set up that we return.

        Verifies that if the replica set is already set up that no attempts to set it up again are
        made and that there are no attempts to set up users.
        """
        # presets
        self.harness.set_leader(True)

        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.return_value = True
        self.harness.charm.unit.get_container = mock_container

        self.harness.charm.app_peer_data["db_initialised"] = "True"
        self.harness.charm.app_peer_data["users_initialized"] = "True"

        self.harness.charm.on.start.emit()

        # when the database has already been initialised we should not set up the replica set or
        # handle users
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_user.assert_not_called()
        provider.return_value.oversee_users.assert_not_called()
        defer.assert_not_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_mongod_not_ready(self, connection, init_user, provider, defer):
        """Tests that if mongod is not ready that we defer and return.

        Verifies that if mongod is not ready that no attempts to set up the replica set and set up
        users are made.
        """
        # presets
        self.harness.set_leader(True)

        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.return_value = True
        self.harness.charm.unit.get_container = mock_container

        connection.return_value.__enter__.return_value.is_ready = False

        self.harness.charm.on.start.emit()

        # when mongod is not ready we should not set up the replica set or handle users
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_user.assert_not_called()
        provider.return_value.oversee_users.assert_not_called()

        # verify app data
        self.assertEqual("db_initialised" in self.harness.charm.app_peer_data, False)
        defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_mongod_error_initalising_replica_set(
        self, connection, init_user, provider, defer
    ):
        """Tests that failure to initialise replica set is properly handled.

        Verifies that when there is a failure to initialise replica set that no operations related
        to setting up users are executed.
        """
        # presets
        self.harness.set_leader(True)

        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.return_value = True
        self.harness.charm.unit.get_container = mock_container
        connection.return_value.__enter__.return_value.is_ready = True

        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            connection.return_value.__enter__.return_value.init_replset.side_effect = exception
            self.harness.charm.on.start.emit()

            init_user.assert_not_called()
            provider.return_value.oversee_users.assert_not_called()

            # verify app data
            self.assertEqual("db_initialised" in self.harness.charm.app_peer_data, False)
            defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_mongod_error_initalising_user(self, connection, init_user, provider, defer):
        """Tests that failure to initialise users set is properly handled.

        Verifies that when there is a failure to initialise users that overseeing users is not
        called.
        """
        # presets
        self.harness.set_leader(True)

        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.return_value = True
        self.harness.charm.unit.get_container = mock_container
        connection.return_value.__enter__.return_value.is_ready = True

        init_user.side_effect = ExecError("command", 0, "stdout", "stderr")
        self.harness.charm.on.start.emit()

        provider.return_value.oversee_users.assert_not_called()
        defer.assert_called()

        # verify app data
        self.assertEqual("users_initialized" in self.harness.charm.app_peer_data, False)

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider")
    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBConnection")
    def test_start_mongod_error_overseeing_users(self, connection, init_user, provider, defer):
        """Tests failures related to pymongo are properly handled when overseeing users.

        Verifies that when there is a failure to oversee users that we defer and do not set the
        data base to initialised.
        """
        # presets
        self.harness.set_leader(True)

        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.return_value = True
        self.harness.charm.unit.get_container = mock_container
        connection.return_value.__enter__.return_value.is_ready = True

        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            provider.side_effect = exception
            self.harness.charm.on.start.emit()

            provider.return_value.oversee_users.assert_not_called()
            defer.assert_called()

            # verify app data
            self.assertEqual("users_initialized" in self.harness.charm.app_peer_data, False)

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBConnection")
    def test_reconfigure_not_already_initialised(self, connection, defer):
        """Tests reconfigure does not execute when database has not been initialised.

        Verifies in case of relation_joined and relation departed, that when the the database has
        not yet been initialised that no attempts to remove/add units are made.
        """
        # presets
        self.harness.set_leader(True)
        rel = self.harness.charm.model.get_relation("database-peers")

        # test both relation events
        for departed in [False, True]:
            if departed:
                # departed presets
                connection.return_value.__enter__.return_value.get_replset_members.return_value = {
                    "mongodb-k8s-0.mongodb-k8s-endpoints",
                    "mongodb-k8s-1.mongodb-k8s-endpoints",
                }

                # simulate removing 2nd MongoDB unit
                self.harness.remove_relation_unit(rel.id, "mongodb-k8s/1")
            else:
                # joining presets
                connection.return_value.__enter__.return_value.get_replset_members.return_value = {
                    "mongodb-k8s-0.mongodb-k8s-endpoints"
                }

                # simulate 2nd MongoDB unit joining
                self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
                self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

            if departed:
                connection.return_value.__enter__.return_value.add_replset_member.assert_not_called()
            else:
                connection.return_value.__enter__.return_value.remove_replset_member.assert_not_called()

            defer.assert_not_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBConnection")
    @patch("charms.mongodb.v0.mongodb.MongoClient")
    def test_reconfigure_get_members_failure(self, client, connection, defer):
        """Tests reconfigure does not execute when unable to get the replica set members.

        Verifies in case of relation_joined and relation departed, that when the the database
        cannot retrieve the replica set members that no attempts to remove/add units are made and
        that the the event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        rel = self.harness.charm.model.get_relation("database-peers")

        for exception, _ in PYMONGO_EXCEPTIONS:
            connection.return_value.__enter__.return_value.get_replset_members.side_effect = (
                exception
            )

            # test both relation events
            for departed in [False, True]:
                if departed:
                    # simulate removing 2nd MongoDB unit
                    self.harness.remove_relation_unit(rel.id, "mongodb-k8s/1")
                else:
                    # simulate 2nd MongoDB unit joining
                    self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
                    self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

                if departed:
                    connection.return_value.__enter__.return_value.add_replset_member.assert_not_called()
                else:
                    connection.return_value.__enter__.return_value.remove_replset_member.assert_not_called()

                defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBConnection")
    def test_reconfigure_remove_member_failure(self, connection, defer):
        """Tests reconfigure does not proceed when unable to remove a member.

        Verifies in relation departed events, that when the database cannot remove a member that
        the event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        connection.return_value.__enter__.return_value.get_replset_members.return_value = {
            "mongodb-k8s-0.mongodb-k8s-endpoints",
            "mongodb-k8s-1.mongodb-k8s-endpoints",
        }
        rel = self.harness.charm.model.get_relation("database-peers")

        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append((NotReadyError, None))
        for exception, _ in exceptions:
            connection.return_value.__enter__.return_value.remove_replset_member.side_effect = (
                exception
            )

            # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
            self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
            self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

            # simulate removing 2nd MongoDB unit
            self.harness.remove_relation_unit(rel.id, "mongodb-k8s/1")

            connection.return_value.__enter__.return_value.remove_replset_member.assert_called()
            defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBConnection")
    def test_reconfigure_peer_not_ready(self, connection, defer):
        """Tests reconfigure does not proceed when the adding member is not ready.

        Verifies in relation joined events, that when the adding member is not ready that the event
        is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        connection.return_value.__enter__.return_value.get_replset_members.return_value = {
            "mongodb-k8s-0.mongodb-k8s-endpoints"
        }
        connection.return_value.__enter__.return_value.is_ready = False

        # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
        rel = self.harness.charm.model.get_relation("database-peers")
        self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
        self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

        connection.return_value.__enter__.return_value.add_replset_member.assert_not_called()
        defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBConnection")
    def test_reconfigure_add_member_failure(self, connection, defer):
        """Tests reconfigure does not proceed when unable to add a member.

        Verifies in relation joined events, that when the database cannot add a member that the
        event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        connection.return_value.__enter__.return_value.get_replset_members.return_value = {
            "mongodb-k8s-0.mongodb-k8s-endpoints"
        }
        rel = self.harness.charm.model.get_relation("database-peers")

        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append((NotReadyError, None))
        for exception, _ in exceptions:
            connection.return_value.__enter__.return_value.add_replset_member.side_effect = (
                exception
            )

            # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
            self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
            self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

            connection.return_value.__enter__.return_value.add_replset_member.assert_called()
            defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider.oversee_users")
    @patch("charm.MongoDBConnection")
    def test_start_init_operator_user_after_second_call(self, connection, oversee_users, defer):
        """Tests that the creation of the admin user is only performed once.

        Verifies that if the user is already set up, that no attempts to set it up again are
        made when a failure happens causing an event deferring calling the init_user again
        """
        mock_container = mock.Mock()
        mock_container.return_value.can_connect.return_value = True
        mock_container.return_value.exists.return_value = True
        mock_container.return_value.exec.return_value = mock.Mock()
        mock_container.return_value.exec.return_value.wait_output.return_value = ("Success", None)

        self.harness.charm.unit.get_container = mock_container

        connection.return_value.__enter__.return_value.is_ready = True

        oversee_users.side_effect = PyMongoError()

        self.harness.charm.on.start.emit()

        # verify app data
        self.assertEqual("operator-user-created" in self.harness.charm.app_peer_data, True)
        defer.assert_called()

        # the second call to init user should fail if "exec" is called, but shouldn't happen
        oversee_users.side_effect = None
        defer.reset_mock()
        mock_container.return_value.exec.reset_mock()
        mock_container.return_value.exec.side_effect = ExecError([], 1, "", "Dummy Error")

        # re-run the start method without a failing oversee_users
        self.harness.charm.on.start.emit()

        # _init_operator_user should have returned before reaching the "exec" call
        mock_container.return_value.exec.assert_not_called()

        defer.assert_not_called()

    def test_get_password(self):
        self._setup_secrets()
        assert isinstance(self.harness.charm.get_secret("app", "monitor-password"), str)
        self.harness.charm.get_secret("app", "non-existing-secret") is None

        self.harness.charm.set_secret("unit", "somekey", "bla")
        assert isinstance(self.harness.charm.get_secret("unit", "somekey"), str)
        self.harness.charm.get_secret("unit", "non-existing-secret") is None

    def test_set_reset_existing_password_app(self):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        self._setup_secrets()

        # Getting current password
        self.harness.charm.set_secret("app", "monitor-password", "bla")
        assert self.harness.charm.get_secret("app", "monitor-password") == "bla"

        self.harness.charm.set_secret("app", "monitor-password", "blablabla")
        assert self.harness.charm.get_secret("app", "monitor-password") == "blablabla"

    @parameterized.expand([("app"), ("unit")])
    def test_set_secret_returning_secret_id(self, scope):
        secret_id = self.harness.charm.set_secret(scope, "somekey", "bla")
        assert re.match(f"mongodb-k8s.{scope}", secret_id)

    @parameterized.expand([("app"), ("unit")])
    def test_set_reset_new_secret(self, scope):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        # Getting current password
        self.harness.charm.set_secret(scope, "new-secret", "bla")
        assert self.harness.charm.get_secret(scope, "new-secret") == "bla"

        # Reset new secret
        self.harness.charm.set_secret(scope, "new-secret", "blablabla")
        assert self.harness.charm.get_secret(scope, "new-secret") == "blablabla"

        # Set another new secret
        self.harness.charm.set_secret(scope, "new-secret2", "blablabla")
        assert self.harness.charm.get_secret(scope, "new-secret2") == "blablabla"

    @parameterized.expand([("app"), ("unit")])
    def test_invalid_secret(self, scope):
        with self.assertRaises(TypeError):
            self.harness.charm.set_secret("unit", "somekey", 1)

        self.harness.charm.set_secret("unit", "somekey", "")
        assert self.harness.charm.get_secret(scope, "somekey") is None

    @pytest.mark.usefixtures("use_caplog")
    def test_delete_password(self):
        """NOTE: currently ops.testing seems to allow for non-leader to remove secrets too!"""
        self._setup_secrets()

        assert self.harness.charm.get_secret("app", "monitor-password")
        self.harness.charm.remove_secret("app", "monitor-password")
        assert self.harness.charm.get_secret("app", "monitor-password") is None

        assert self.harness.charm.set_secret("unit", "somekey", "somesecret")
        self.harness.charm.remove_secret("unit", "somekey")
        assert self.harness.charm.get_secret("unit", "somekey") is None

        with self._caplog.at_level(logging.ERROR):
            self.harness.charm.remove_secret("app", "monitor-password")
            assert (
                "Non-existing secret app:monitor-password was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.remove_secret("unit", "somekey")
            assert (
                "Non-existing secret unit:somekey was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.remove_secret("app", "non-existing-secret")
            assert (
                "Non-existing secret app:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.remove_secret("unit", "non-existing-secret")
            assert (
                "Non-existing secret unit:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

    @parameterized.expand([("app"), ("unit")])
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    def test_on_secret_changed(self, scope, connect_exporter):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        secret_label = self.harness.charm.set_secret(scope, "new-secret", "bla")
        secret = self.harness.charm.model.get_secret(label=secret_label)

        event = mock.Mock()
        event.secret = secret
        secret_label = self.harness.charm._on_secret_changed(event)
        connect_exporter.assert_called()

    @parameterized.expand([("app"), ("unit")])
    @pytest.mark.usefixtures("use_caplog")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    def test_on_other_secret_changed(self, scope, connect_exporter):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        # "Hack": creating a secret outside of the normal MongoDBCharm.set_secret workflow
        scope_obj = self.harness.charm._scope_opj(scope)
        secret = scope_obj.add_secret({"key": "value"})

        event = mock.Mock()
        event.secret = secret

        with self._caplog.at_level(logging.DEBUG):
            self.harness.charm._on_secret_changed(event)
            assert f"Secret {secret.id} changed, but it's unknown" in self._caplog.text

        connect_exporter.assert_not_called()

    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    def test_connect_to_mongo_exporter_on_set_password(self, connect_exporter, connection):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        self.harness.charm.on.mongod_pebble_ready.emit(container)
        self.harness.set_leader(True)

        action_event = mock.Mock()
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_set_password(action_event)
        connect_exporter.assert_called()

    @patch("charm.MongoDBBackups._get_pbm_status")
    @patch("charm.MongoDBCharm.has_backup_service")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    def test_event_set_password_secrets(
        self, connect_exporter, connection, has_backup_service, get_pbm_status
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        pw = "bla"
        has_backup_service.return_value = True
        get_pbm_status.return_value = ActiveStatus()
        self.harness.set_leader(True)

        action_event = mock.Mock()
        action_event.set_results = MagicMock()
        action_event.params = {"username": "monitor", "password": pw}
        self.harness.charm._on_set_password(action_event)
        connect_exporter.assert_called()

        action_event.set_results.assert_called()
        args_pw_set = action_event.set_results.call_args.args[0]
        assert "secret-id" in args_pw_set

        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        assert args_pw["password"] == pw

    @patch("charm.MongoDBBackups._get_pbm_status")
    @patch("charm.MongoDBCharm.has_backup_service")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    def test_event_auto_reset_password_secrets_when_no_pw_value_shipped(
        self, connect_exporter, connection, has_backup_service, get_pbm_status
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        has_backup_service.return_value = True
        get_pbm_status.return_value = ActiveStatus()
        self._setup_secrets()
        self.harness.set_leader(True)

        action_event = mock.Mock()
        action_event.set_results = MagicMock()

        # Getting current password
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        pw1 = args_pw["password"]

        # No password value was shipped
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_set_password(action_event)
        connect_exporter.assert_called()

        # New password was generated
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        pw2 = args_pw["password"]

        # a new password was created
        assert pw1 != pw2

    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    def test_event_any_unit_can_get_password_secrets(self, connect_exporter, connection):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        self._setup_secrets()

        action_event = mock.Mock()
        action_event.set_results = MagicMock()

        # Getting current password
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        assert args_pw["password"]

    @patch("charm.MongoDBCharm._pull_licenses")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm._set_data_dir_permissions")
    @patch("charm.MongoDBConnection")
    def test__connect_mongodb_exporter_success(
        self, connection, fix_data_dir, defer, pull_licenses
    ):
        """Tests the _connect_mongodb_exporter method has been called."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        self.harness.charm.on.mongod_pebble_ready.emit(container)
        password = self.harness.charm.get_secret("app", "monitor-password")

        uri_template = "mongodb://monitor:{password}@mongodb-k8s-0.mongodb-k8s-endpoints/?replicaSet=mongodb-k8s&authSource=admin"

        expected_config = {
            "override": "replace",
            "summary": "mongodb_exporter",
            "command": "mongodb_exporter --collector.diagnosticdata --compatible-mode",
            "startup": "enabled",
            "user": "mongodb",
            "group": "mongodb",
            "environment": {"MONGODB_URI": uri_template.format(password=password)},
        }

        container_plan = self.harness.get_container_pebble_plan("mongod").to_dict()
        exporter_config = container_plan.get("services").get("mongodb_exporter")
        self.assertEqual(expected_config, exporter_config)

        service = self.harness.model.unit.get_container("mongod").get_service("mongodb_exporter")
        assert service.is_running()

        action_event = mock.Mock()
        action_event.params = {"username": "monitor", "password": "mongo123"}
        self.harness.charm._on_set_password(action_event)
        password = self.harness.charm.get_secret("app", "monitor-password")

        updated_plan = self.harness.get_container_pebble_plan("mongod").to_dict()
        new_uri = (
            updated_plan.get("services")
            .get("mongodb_exporter")
            .get("environment")
            .get("MONGODB_URI")
        )
        expected_uri = uri_template.format(password="mongo123")
        self.assertEqual(expected_uri, new_uri)

    @patch("charm.MongoDBCharm._init_operator_user")
    @patch("charm.MongoDBCharm._init_monitor_user")
    @patch("charm.MongoDBCharm._connect_mongodb_exporter")
    @patch("ops.model.Container.exists")
    @patch("charm.MongoDBCharm._pull_licenses")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm._set_data_dir_permissions")
    @patch("charm.MongoDBConnection")
    def test__backup_user_created(
        self,
        connection,
        fix_data_dir,
        defer,
        pull_licenses,
        _socket_exists,
        _connect_mongodb_exporter,
        _init_operator_user,
        _init_monitor_user,
    ):
        """Tests what backup user was created."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        self.harness.charm.on.start.emit()
        password = self.harness.charm.get_secret("app", "backup-password")
        self.assertIsNotNone(password)  # verify the password is set

    @patch("charm.MongoDBConnection")
    def test_set_password_provided(self, connection):
        """Tests that a given password is set as the new mongodb password for backup user."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_leader(True)
        self.harness.set_can_connect(container, True)
        self.harness.charm.on.start.emit()
        action_event = mock.Mock()
        action_event.params = {"password": "canonical123", "username": "backup"}
        self.harness.charm._on_set_password(action_event)
        new_password = self.harness.charm.get_secret("app", "backup-password")

        # verify app data is updated and results are reported to user
        self.assertEqual("canonical123", new_password)

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBCharm.has_backup_service")
    @patch("charm.MongoDBBackups._get_pbm_status")
    def test_set_backup_password_pbm_busy(self, pbm_status, has_backup_service):
        """Tests changes to passwords fail when pbm is restoring/backing up."""
        self.harness.set_leader(True)
        original_password = "pass123"
        action_event = mock.Mock()
        has_backup_service.return_value = True

        for username in ["backup", "monitor", "operator"]:
            self.harness.charm.app_peer_data[f"{username}-password"] = original_password
            action_event.params = {"username": username}
            pbm_status.return_value = MaintenanceStatus("pbm")
            self.harness.charm._on_set_password(action_event)
            current_password = self.harness.charm.app_peer_data[f"{username}-password"]
            action_event.fail.assert_called()
            self.assertEqual(current_password, original_password)
