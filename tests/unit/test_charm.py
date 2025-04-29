# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import unittest
from unittest import mock
from unittest.mock import patch

import pytest
from data_platform_helpers.advanced_statuses.models import StatusObject
from ops.model import MaintenanceStatus
from ops.pebble import PathError, ProtocolError
from ops.testing import ActionFailed, Harness
from parameterized import parameterized
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure
from single_kernel_mongo.config.literals import Scope
from single_kernel_mongo.core.structured_config import MongoDBRoles
from single_kernel_mongo.exceptions import WorkloadExecError
from single_kernel_mongo.utils.mongo_connection import NotReadyError
from single_kernel_mongo.utils.mongodb_users import (
    BackupUser,
    MonitorUser,
    OperatorUser,
)

from charm import MongoDBK8sCharm

PYMONGO_EXCEPTIONS = [
    (ConnectionFailure("error message"), ConnectionFailure),
    (ConfigurationError("error message"), ConfigurationError),
    (OperationFailure("error message"), OperationFailure),
]
PEER_ADDR = {"private-address": "127.4.5.6"}

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def patch_upgrades(monkeypatch):
    monkeypatch.setattr(
        "single_kernel_mongo.state.charm_state.CharmState.upgrade_in_progress", False
    )
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.get_partition",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.set_partition",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.get_pod",
        lambda *args, **kwargs: 0,
    )


@pytest.fixture(autouse=True)
def patch_is_ready(mocker):
    mocker.patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
        return_value=True,
    )


class TestCharm(unittest.TestCase):
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    def setUp(self, *unused):
        self.maxDiff = None
        self.harness = Harness(MongoDBK8sCharm)
        mongo_resource = {
            "registrypath": "mongo:4.4",
        }
        self.harness.add_oci_resource("mongodb-image", mongo_resource)
        self.harness.add_relation("database-peers", "mongodb-peers")
        self.harness.add_relation("upgrade-version-a", "upgrade-version-a")
        self.harness.begin()
        with self.harness.hooks_disabled():
            self.harness.add_storage(storage_name="mongodb", count=1, attach=True)
            self.harness.add_storage(storage_name="mongodb-logs", count=1, attach=True)
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    @pytest.fixture
    def use_caplog(self, caplog):
        self._caplog = caplog

    def _setup_secrets(self):
        self.harness.set_leader(True)
        self.harness.set_leader(False)

    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator._initialise_replica_set")
    @patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.exec")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_mongod_pebble_ready(
        self, connect_exporter, fix_data_dir, defer, pull_licenses, exec, init_repl
    ):
        # Expected plan after Pebble ready with default config
        expected_plan = {
            "services": {
                "logrotate": {
                    "summary": "log rotate",
                    "startup": "enabled",
                    "override": "replace",
                    "command": "sh -c 'logrotate /etc/logrotate.d/mongodb; sleep 1'",
                    "user": "mongodb",
                    "group": "mongodb",
                    "backoff-delay": "1m0s",
                    "backoff-factor": 1,
                },
                "mongod": {
                    "user": "mongodb",
                    "group": "mongodb",
                    "override": "replace",
                    "summary": "mongod",
                    "command": "/bin/bash /bin/start-mongod.sh",
                    "environment": {"MONGOD_ARGS": ""},
                    "startup": "enabled",
                },
            },
        }
        # Get the mongod container from the model
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        container.make_dir("/etc/logrotate.d", make_parents=True)
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
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    def test_pebble_ready_container_cannot_connect(self, push_keyfile_to_workload, defer, *unused):
        """Test verifies behavior when cannot connect to container in pebble ready function.

        Verifies that when a failure to connect to container results in a deferral and that no
        efforts to set keyFile or add/replan layers are made.
        """
        # presets
        self.harness.set_leader(True)
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, False)

        # Emit the PebbleReadyEvent carrying the mongod container
        self.harness.charm.on.mongod_pebble_ready.emit(container)

        push_keyfile_to_workload.assert_not_called()
        defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    def test_pebble_ready_push_keyfile_to_workload_failure(
        self, set_perms, push_keyfile_to_workload, defer, *unused
    ):
        """Test verifies behavior when setting keyfile fails.

        Verifies that when a failure to set keyfile occurs that there is no attempt to add layers
        or replan the container.
        """
        # presets
        self.harness.set_leader(True)
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        for exception in [
            PathError("kind", "message"),
            ProtocolError("kind", "message"),
        ]:
            push_keyfile_to_workload.side_effect = exception

            # Emit the PebbleReadyEvent carrying the mongod container
            self.harness.charm.on.mongod_pebble_ready.emit(container)

            set_perms.assert_not_called()
            defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator._configure_workloads")
    def test_pebble_ready_no_storage_yet(self, configure, defer):
        """Test to ensure that the pebble ready event is deferred until the storage is ready."""
        # presets
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        # Mock storages
        self.harness.charm.model._storages = {"mongodb": None, "mongodb-logs": None}
        # Emit the PebbleReadyEvent carrying the mock_container
        self.harness.charm.on.mongod_pebble_ready.emit(container)
        configure.assert_not_called()
        defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_replica_set")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_charm_admin_users")
    def test_start_container_cannot_connect(self, init_users, init_replset, defer, *unused):
        """Tests inability to connect results in deferral.

        Verifies that if connection is not possible, that there are no attempts to set up the
        replica set or handle users.
        """
        # presets
        self.harness.set_leader(True)
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, False)

        self.harness.charm.on.start.emit()

        # when cannot connect to container we should not set up the replica set or handle users
        init_replset.assert_not_called()
        init_users.assert_not_called()

        # verify app data
        self.assertEqual(self.harness.charm.operator.state.db_initialised, False)
        defer.assert_called()

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.MongoDBOperator._configure_workloads",
        return_value=None,
    )
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_replica_set")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_charm_admin_users")
    def test_start_already_initialised(self, init_user, init_replset, defer, *unused):
        """Tests that if the replica set has already been set up that we return.

        Verifies that if the replica set is already set up that no attempts to set it up again are
        made and that there are no attempts to set up users.
        """
        # presets
        self.harness.set_leader(True)

        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        self.harness.charm.operator.state.db_initialised = True

        self.harness.charm.on.start.emit()

        # when the database has already been initialised we should not set up the replica set or
        # handle users
        init_replset.assert_not_called()
        init_user.assert_not_called()
        defer.assert_not_called()

    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_replica_set")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
        new_callable=mock.PropertyMock(return_value=False),
    )
    def test_start_mongod_not_ready(
        self, is_ready, init_replset, set_perms, handle_licenses, defer
    ):
        """Tests that if mongod is not ready that we defer and return.

        Verifies that if mongod is not ready that no attempts to set up the replica set and set up
        users are made.
        """
        # presets
        self.harness.set_leader(True)

        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        self.harness.charm.on.start.emit()

        # when mongod is not ready we should not set up the replica set or handle users
        init_replset.assert_not_called()

        # verify app data
        self.assertEqual(self.harness.charm.operator.state.db_initialised, False)
        defer.assert_called()

    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_charm_admin_users")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.init_replset")
    @patch("ops.framework.EventBase.defer")
    def test_start_mongod_error_initialising_replica_set(
        self, defer, init_replset, init_charm_user, *unused
    ):
        """Tests that failure to initialise replica set is properly handled.

        Verifies that when there is a failure to initialise replica set the defer is called and
        db_initialised is not set to initialised.
        """
        # presets
        self.harness.set_leader(True)

        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        for exception, _ in PYMONGO_EXCEPTIONS:
            init_replset.side_effect = exception
            self.harness.charm.on.start.emit()

            # verify app data
            self.assertEqual(self.harness.charm.operator.state.db_initialised, False)
            init_charm_user.assert_not_called()
            defer.assert_called()

    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_operator_user")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.initialise_user")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.init_replset")
    def test_error_initialising_users(
        self,
        init_replset,
        init_user,
        init_operator_user,
        set_perms,
        handle_licenses,
        defer,
        *unused,
    ):
        """Tests that failure to initialise users set is properly handled.

        Verifies that when there is a failure to initialise users that overseeing users is not
        called.
        """
        # presets
        self.harness.set_leader(True)

        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        init_operator_user.side_effect = WorkloadExecError("command", 0, "stdout", "stderr")
        self.harness.charm.on.start.emit()

        init_user.assert_not_called()
        defer.assert_called()

        # verify app data
        self.assertEqual(self.harness.charm.operator.state.db_initialised, False)

    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    @patch(
        "single_kernel_mongo.managers.mongo.MongoManager.initialise_replica_set",
    )
    @patch(
        "single_kernel_mongo.managers.mongo.MongoManager.initialise_charm_admin_users",
    )
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.user_exists")
    def test_start_mongod_error_overseeing_users(self, user_exists, defer, *unused):
        """Tests failures related to pymongo are properly handled when overseeing users.

        Verifies that when there is a failure to oversee users that we defer and do not set the
        data base to initialised.
        """
        # presets
        self.harness.set_leader(True)

        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        self.harness.charm.operator.state.app_peer_data.role = MongoDBRoles.REPLICATION

        self.harness.add_relation("database", "client-app")

        for exception, _ in PYMONGO_EXCEPTIONS:
            user_exists.side_effect = exception
            self.harness.charm.on.start.emit()

            defer.assert_called()

            # verify app data
            self.assertEqual(self.harness.charm.operator.state.db_initialised, False)

    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection")
    def test_reconfigure_not_already_initialised(self, connection, defer, *unused):
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

    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_members")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.add_replset_member")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.remove_replset_member")
    def test_reconfigure_get_members_failure(
        self, remove_replset, add_replset, get_replset, defer, *unused
    ):
        """Tests reconfigure does not execute when unable to get the replica set members.

        Verifies in case of relation_joined and relation departed, that when the the database
        cannot retrieve the replica set members that no attempts to remove/add units are made and
        that the the event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        rel = self.harness.charm.model.get_relation("database-peers")

        for exception, _ in PYMONGO_EXCEPTIONS:
            get_replset.side_effect = exception

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
                    add_replset.assert_not_called()
                else:
                    remove_replset.assert_not_called()

                defer.assert_called()

    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    @patch("single_kernel_mongo.managers.config.BackupConfigManager.configure_and_restart")
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_members")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.add_replset_member")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.remove_replset_member")
    def test_reconfigure_remove_member_failure(
        self,
        remove_replset_member,
        add_replset_member,
        get_replset_members,
        defer,
        *unused,
    ):
        """Tests reconfigure does not proceed when unable to remove a member.

        Verifies in relation departed events, that when the database cannot remove a member that
        the event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        get_replset_members.return_value = {
            "mongodb-k8s-0.mongodb-k8s-endpoints",
            "mongodb-k8s-1.mongodb-k8s-endpoints",
        }
        rel = self.harness.charm.model.get_relation("database-peers")

        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append((NotReadyError, None))
        for exception, _ in exceptions:
            remove_replset_member.side_effect = exception

            # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
            self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
            self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

            # simulate removing 2nd MongoDB unit
            self.harness.remove_relation_unit(rel.id, "mongodb-k8s/1")

            remove_replset_member.assert_called()
            defer.assert_called()

    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
        new_callable=mock.PropertyMock(return_value=False),
    )
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_members")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.add_replset_member")
    def test_reconfigure_peer_not_ready(
        self,
        add_replset_member,
        get_replset_members,
        defer,
        *unused,
    ):
        """Tests reconfigure does not proceed when the adding member is not ready.

        Verifies in relation joined events, that when the adding member is not ready that the event
        is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        get_replset_members.return_value = {"mongodb-k8s-0.mongodb-k8s-endpoints"}

        # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
        rel = self.harness.charm.model.get_relation("database-peers")
        self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
        self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

        add_replset_member.assert_not_called()
        defer.assert_called()

    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_members")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.add_replset_member")
    def test_reconfigure_add_member_failure(self, add_replset, get_replset, defer, *unused):
        """Tests reconfigure does not proceed when unable to add a member.

        Verifies in relation joined events, that when the database cannot add a member that the
        event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        get_replset.return_value = {"mongodb-k8s-0.mongodb-k8s-endpoints"}
        rel = self.harness.charm.model.get_relation("database-peers")

        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append((NotReadyError, None))
        for exception, _ in exceptions:
            add_replset.side_effect = exception

            # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
            self.harness.add_relation_unit(rel.id, "mongodb-k8s/1")
            self.harness.update_relation_data(rel.id, "mongodb-k8s/1", PEER_ADDR)

            add_replset.assert_called()
            defer.assert_called()

    def test_get_password(self):
        self.harness.set_leader(True)

        assert isinstance(
            self.harness.charm.operator.state.secrets.get_for_key(Scope.APP, "monitor-password"),
            str,
        )
        assert (
            self.harness.charm.operator.state.secrets.get_for_key(Scope.APP, "non-existing")
            is None
        )

        self.harness.charm.operator.state.secrets.set("somekey", "bla", Scope.UNIT)
        assert isinstance(
            self.harness.charm.operator.state.secrets.get_for_key(Scope.UNIT, "somekey"),
            str,
        )
        assert (
            self.harness.charm.operator.state.secrets.get_for_key(Scope.APP, "non-existing")
            is None
        )

    def test_delete_password_non_leader(self):
        self._setup_secrets()
        self.harness.set_leader(False)
        assert self.harness.charm.operator.state.get_user_password(MonitorUser)
        with self.assertRaises(RuntimeError):
            self.harness.charm.operator.state.secrets.remove(Scope.APP, "monitor-password")

    @parameterized.expand([(Scope.APP), (Scope.UNIT)])
    def test_invalid_secret(self, scope):
        with self.assertRaises(TypeError):
            self.harness.charm.operator.state.secrets.set("somekey", 1, Scope.UNIT)

        self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "somekey")
        assert self.harness.charm.operator.state.secrets.get_for_key(scope, "somekey") is None

    @pytest.mark.usefixtures("use_caplog")
    def test_delete_password(self):
        self.harness.set_leader(True)

        assert self.harness.charm.operator.state.get_user_password(MonitorUser)
        self.harness.charm.operator.state.secrets.remove(Scope.APP, "monitor-password")
        assert self.harness.charm.operator.state.get_user_password(MonitorUser) == ""

        assert self.harness.charm.operator.state.secrets.set("somekey", "somesecret", Scope.UNIT)
        self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "somekey")
        assert self.harness.charm.operator.state.secrets.get_for_key(Scope.UNIT, "somekey") is None

        with self._caplog.at_level(logging.ERROR):
            self.harness.charm.operator.state.secrets.remove(Scope.APP, "monitor-password")
            assert (
                "Non-existing secret app:monitor-password was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "somekey")
            assert (
                "Non-existing secret unit:somekey was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.operator.state.secrets.remove(Scope.APP, "non-existing-secret")
            assert (
                "Non-existing secret app:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "non-existing-secret")
            assert (
                "Non-existing secret unit:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

    @parameterized.expand([(Scope.APP), (Scope.UNIT)])
    @patch("single_kernel_mongo.managers.config.BackupConfigManager.configure_and_restart")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_on_secret_changed(self, scope, connect_exporter, connect_backup):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        secret = self.harness.charm.operator.state.secrets.set("new-secret", "bla", scope)
        secret = self.harness.charm.model.get_secret(label=secret.label)

        self.harness.charm.on.secret_changed.emit(label=secret.label, id=secret.id)
        connect_exporter.assert_called()
        connect_backup.assert_called()

    @parameterized.expand([(Scope.APP), (Scope.UNIT)])
    @pytest.mark.usefixtures("use_caplog")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_on_other_secret_changed(self, scope, connect_exporter):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        # "Hack": creating a secret outside of the normal MongodbOperatorCharm.set_secret workflow
        scope_obj = self.harness.charm.app if scope == Scope.APP else self.harness.charm.unit
        secret = scope_obj.add_secret({"key": "value"})

        with self._caplog.at_level(logging.DEBUG):
            self.harness.charm.on.secret_changed.emit(label=secret.label, id=secret.id)
            assert f"Secret {secret.id} changed, but it's unknown" in self._caplog.text

        connect_exporter.assert_not_called()

    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_connect_to_mongo_exporter_on_set_password(
        self, connect_exporter, mock_set_user_password
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'monitor' user."""
        self.harness.set_leader(True)

        self.harness.run_action("set-password", {"username": "monitor"})
        connect_exporter.assert_called()

    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_event_auto_reset_password_secrets_when_no_pw_value_shipped(
        self, connect_exporter, set_user_password
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        self.harness.set_leader(True)

        # Getting current password
        params = {"username": "monitor"}
        output = self.harness.run_action("set-password", params)
        assert output.results["password"]
        pw1 = output.results["password"]

        connect_exporter.assert_called()

        # New password was generated
        params = {"username": "monitor"}
        output = self.harness.run_action("set-password", params)
        assert output.results["password"]
        pw2 = output.results["password"]

        # a new password was created
        assert pw1 != pw2

    @patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.exec")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    @patch("ops.framework.EventBase.defer")
    def test__connect_mongodb_exporter_success(self, defer, *unused):
        """Tests the _connect_mongodb_exporter method has been called."""
        # Get container
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        container.make_dir("/etc/logrotate.d", make_parents=True)

        self.harness.charm.operator.state.db_initialised = True
        self.harness.charm.on.mongod_pebble_ready.emit(container)

        password = self.harness.charm.operator.state.get_user_password(MonitorUser)

        uri_template = "mongodb://monitor:{password}@mongodb-k8s-0.mongodb-k8s-endpoints:27017/admin?replicaSet=mongodb-k8s"

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
        exporter_config = container_plan.get("services").get("mongodb-exporter")
        self.assertEqual(expected_config, exporter_config)

        service = self.harness.model.unit.get_container("mongod").get_service("mongodb-exporter")
        assert service.is_running()

        params = {"username": "monitor", "password": "mongo123"}
        self.harness.run_action("set-password", params)

        password = self.harness.charm.operator.state.get_user_password(MonitorUser)

        updated_plan = self.harness.get_container_pebble_plan("mongod").to_dict()
        new_uri = (
            updated_plan.get("services")
            .get("mongodb-exporter")
            .get("environment")
            .get("MONGODB_URI")
        )
        expected_uri = uri_template.format(password="mongo123")
        self.assertEqual(expected_uri, new_uri)

    @patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.exec")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    def test_set_password_provided(self, *unused):
        """Tests that a given password is set as the new mongodb password for backup user."""
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        params = {"password": "canonical123", "username": "backup"}
        self.harness.run_action("set-password", params)
        new_password = self.harness.charm.operator.state.get_user_password(BackupUser)

        # verify app data is updated and results are reported to user
        self.assertEqual("canonical123", new_password)

    @patch("single_kernel_mongo.managers.backups.BackupManager.compute_statuses")
    def test_set_backup_password_pbm_busy(self, pbm_status):
        """Tests changes to passwords fail when pbm is restoring/backing up."""
        self.harness.set_leader(True)

        pbm_status.return_value = [StatusObject(status=MaintenanceStatus("pbm"))]

        for user in [BackupUser, MonitorUser, OperatorUser]:
            original_password = self.harness.charm.operator.state.get_user_password(user)
            with pytest.raises(ActionFailed):
                self.harness.run_action("set-password", {"username": user.username})
            current_password = self.harness.charm.operator.state.get_user_password(user)
            self.assertEqual(current_password, original_password)
