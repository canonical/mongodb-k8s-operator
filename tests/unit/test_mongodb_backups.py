# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from subprocess import CalledProcessError
from unittest import mock
from unittest.mock import patch

import tenacity
from charms.mongodb.v0.mongodb_backups import (
    PBMBusyError,
    ResyncError,
    SetPBMConfigError,
    stop_after_attempt,
    wait_fixed,
)
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    ModelError,
    WaitingStatus,
)
from ops.pebble import ExecError
from ops.testing import Harness

from charm import MongoDBCharm

from .helpers import patch_network_get

RELATION_NAME = "s3-credentials"


class TestMongoBackups(unittest.TestCase):
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self):
        self.harness = Harness(MongoDBCharm)
        self.harness.begin()
        self.harness.add_relation("database-peers", "database-peers")
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_get_pbm_status_snap_not_present(self, pbm_command, service):
        """Tests that when the snap is not present pbm is in blocked state."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        pbm_command.side_effect = ModelError("service pbm-agent not found")
        self.assertTrue(isinstance(self.harness.charm.backups._get_pbm_status(), BlockedStatus))

    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_get_pbm_status_resync(self, pbm_command, service):
        """Tests that when pbm is resyncing that pbm is in waiting state."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"
        pbm_command.return_value = "Currently running:\n====\nResync op"
        self.assertTrue(isinstance(self.harness.charm.backups._get_pbm_status(), WaitingStatus))

    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_get_pbm_status_running(self, pbm_command, service):
        """Tests that when pbm not running an op that pbm is in active state."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"
        pbm_command.return_value = b"Currently running:\n====\n(none)"
        self.assertTrue(isinstance(self.harness.charm.backups._get_pbm_status(), ActiveStatus))

    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_get_pbm_status_incorrect_cred(self, pbm_command, service):
        """Tests that when pbm has incorrect credentials that pbm is in blocked state."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"
        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "status"], exit_code=1, stdout="status code: 403", stderr=""
        )
        self.assertTrue(isinstance(self.harness.charm.backups._get_pbm_status(), BlockedStatus))

    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_get_pbm_status_incorrect_conf(self, pbm_command, service):
        """Tests that when pbm has incorrect configs that pbm is in blocked state."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"
        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "status"], exit_code=1, stdout="status code: 404", stderr=""
        )
        self.assertTrue(isinstance(self.harness.charm.backups._get_pbm_status(), BlockedStatus))

    @patch("charms.mongodb.v0.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v0.mongodb_backups.stop_after_attempt")
    @patch("ops.model.Container.start")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_verify_resync_config_error(self, pbm_command, service, start, retry_wait, retry_stop):
        """Tests that when pbm cannot perform the resync command it raises an error."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"
        pbm_command.side_effect = ExecError(
            command=["pbm status"], exit_code=1, stdout="", stderr=""
        )

        retry_stop.return_value = stop_after_attempt(1)
        retry_wait.return_value = wait_fixed(1)

        with self.assertRaises(ExecError):
            self.harness.charm.backups._resync_config_options()

    @patch("charms.mongodb.v0.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v0.mongodb_backups.stop_after_attempt")
    @patch("ops.model.Container.start")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_verify_resync_cred_error(self, pbm_command, service, start, retry_wait, retry_stop):
        """Tests that when pbm cannot resync due to creds that it raises an error."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"

        retry_stop.return_value = stop_after_attempt(1)
        retry_wait.return_value = wait_fixed(1)
        pbm_command.side_effect = ExecError(
            command=["pbm status"], exit_code=1, stdout="status code: 403", stderr=""
        )

        with self.assertRaises(ExecError):
            self.harness.charm.backups._resync_config_options()

    @patch("ops.model.Container.restart")
    @patch("ops.model.Container.start")
    @patch("charm.MongoDBBackups.substrate")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    @patch("charm.MongoDBBackups._get_pbm_status")
    def test_verify_resync_syncing(
        self, pbm_status, run_pbm_command, service, substrate, start, restart
    ):
        """Tests that when pbm is syncing that it raises an error."""
        substrate.return_value = "k8s"
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"

        pbm_status.return_value = "Currently running:\n====\nResync op"
        run_pbm_command.return_value = "Currently running:\n====\nResync op"

        # disable retry
        self.harness.charm.backups._wait_pbm_status.retry.retry = tenacity.retry_if_not_result(
            lambda x: True
        )

        with self.assertRaises(ResyncError):
            self.harness.charm.backups._resync_config_options()

    @patch("ops.model.Container.start")
    @patch("charms.mongodb.v0.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v0.mongodb_backups.stop_after_attempt")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBBackups._get_pbm_status")
    def test_resync_config_options_failure(
        self, pbm_status, service, retry_stop, retry_wait, start
    ):
        """Verifies _resync_config_options raises an error when a resync cannot be performed."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        service.return_value = "pbm"
        pbm_status.return_value = MaintenanceStatus()

        with self.assertRaises(PBMBusyError):
            self.harness.charm.backups._resync_config_options()

    @patch("charms.mongodb.v0.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v0.mongodb_backups.stop_after_attempt")
    @patch("ops.model.Container.restart")
    @patch("ops.model.Container.start")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBBackups._get_pbm_status")
    def test_resync_config_restart(
        self, pbm_status, service, start, restart, retry_stop, retry_wait
    ):
        """Verifies _resync_config_options restarts that snap if alreaady resyncing."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)

        service.return_value = "pbm"

        retry_stop.return_value = stop_after_attempt(1)
        retry_stop.return_value = wait_fixed(1)
        pbm_status.return_value = WaitingStatus()

        with self.assertRaises(PBMBusyError):
            self.harness.charm.backups._resync_config_options()

        container.restart.assert_called()

    @patch("charm.MongoDBBackups.substrate")
    @patch("charm.MongoDBBackups._get_pbm_configs")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_set_config_options(self, pbm_command, pbm_configs, substrate):
        """Verifies _set_config_options failure raises SetPBMConfigError."""
        substrate.return_value = "k8s"
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        pbm_command.side_effect = [
            None,
            ExecError(
                command=["/usr/bin/pbm config --set this_key=doesnt_exist"],
                stdout="",
                exit_code=42,
                stderr="",
            ),
        ]
        with self.assertRaises(SetPBMConfigError):
            self.harness.charm.backups._set_config_options({"this_key": "doesnt_exist"})

    def test_backup_without_rel(self):
        """Verifies no backups are attempted without s3 relation."""
        action_event = mock.Mock()
        action_event.params = {}

        self.harness.charm.backups._on_create_backup_action(action_event)
        action_event.fail.assert_called()

    @patch("ops.framework.EventBase.defer")
    def test_s3_credentials_no_db(self, defer):
        """Verifies that when there is no DB that setting credentials is deferred."""
        del self.harness.charm.app_peer_data["db_initialised"]

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups.substrate")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBBackups._set_config_options")
    def test_s3_credentials_set_pbm_failure(self, _set_config_options, service, substrate):
        """Test charm goes into blocked state when setting pbm configs fail."""
        substrate.return_value = "k8s"
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"

        _set_config_options.side_effect = SetPBMConfigError
        self.harness.charm.app_peer_data["db_initialised"] = "True"

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBBackups._get_pbm_status")
    def test_s3_credentials_config_error(
        self, pbm_status, service, defer, resync, _set_config_options
    ):
        """Test charm defers when more time is needed to sync pbm."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        service.return_value = "pbm"
        pbm_status.return_value = ActiveStatus()
        resync.side_effect = SetPBMConfigError

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )
        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBBackups._get_pbm_status")
    def test_s3_credentials_syncing(self, pbm_status, service, defer, resync, _set_config_options):
        """Test charm defers when more time is needed to sync pbm credentials."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        service.return_value = "pbm"
        resync.side_effect = ResyncError

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_called()
        self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBBackups._get_pbm_status")
    def test_s3_credentials_pbm_busy(
        self, pbm_status, service, defer, resync, _set_config_options
    ):
        """Test charm defers when more time is needed to sync pbm."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        service.return_value = "pbm"

        resync.side_effect = PBMBusyError

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_called()
        self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm.get_backup_service")
    def test_s3_credentials_snap_start_error(self, service, defer, resync, _set_config_options):
        """Test charm defers when more time is needed to sync pbm."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        service.return_value = "pbm"

        resync.side_effect = RuntimeError

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_not_called()
        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBCharm.get_backup_service")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_s3_credentials_pbm_error(
        self, pbm_command, service, defer, resync, _set_config_options
    ):
        """Test charm defers when more time is needed to sync pbm."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        service.return_value = "pbm"
        self.harness.charm.app_peer_data["db_initialised"] = "True"
        resync.side_effect = ExecError(
            command=["/usr/bin/pbm status"], exit_code=1, stdout="status code: 403", stderr=""
        )
        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm status"], exit_code=1, stdout="status code: 403", stderr=""
        )

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_not_called()
        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @unittest.skip("Not implemented yet")
    @patch("charm.MongoDBBackups._get_pbm_status")
    @patch("charm.snap.SnapCache")
    def test_backup_failed(self, snap, pbm_status, output):
        """Verifies backup is fails if the pbm command failed."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {}
        pbm_status.return_value = ActiveStatus("")

        output.side_effect = CalledProcessError(cmd="charmed-mongodb.pbm backup", returncode=42)

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)

        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    def test_backup_list_without_rel(self):
        """Verifies no backup lists are attempted without s3 relation."""
        action_event = mock.Mock()
        action_event.params = {}

        self.harness.charm.backups._on_list_backups_action(action_event)
        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.snap.SnapCache")
    def test_backup_list_syncing(self, snap, output):
        """Verifies backup list is deferred if more time is needed to resync."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {}
        output.return_value = b"Currently running:\n====\nResync op"

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_list_backups_action(action_event)

        action_event.defer.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.snap.SnapCache")
    def test_backup_list_wrong_cred(self, snap, output):
        """Verifies backup list fails with wrong credentials."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {}
        output.side_effect = CalledProcessError(
            cmd="charmed-mongodb.pbm status", returncode=403, output=b"status code: 403"
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_list_backups_action(action_event)
        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.MongoDBBackups._get_pbm_status")
    @patch("charm.snap.SnapCache")
    def test_backup_list_failed(self, snap, pbm_status, output):
        """Verifies backup list fails if the pbm command fails."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {}
        pbm_status.return_value = ActiveStatus("")

        output.side_effect = CalledProcessError(cmd="charmed-mongodb.pbm list", returncode=42)

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_list_backups_action(action_event)

        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    def test_generate_backup_list_output(self, check_output):
        """Tests correct formation of backup list output.

        Specifically the spacing of the backups, the header, the backup order, and the backup
        contents.
        """
        # case 1: running backup is listed in error state
        with open("tests/unit/data/pbm_status_duplicate_running.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        check_output.return_value = output_contents.encode("utf-8")
        formatted_output = self.harness.charm.backups._generate_backup_list_output()
        formatted_output = formatted_output.split("\n")
        header = formatted_output[0]
        self.assertEqual(header, "backup-id             | backup-type  | backup-status")
        divider = formatted_output[1]
        self.assertEqual(divider, "-" * len(header))
        eariest_backup = formatted_output[2]
        self.assertEqual(
            eariest_backup,
            "1900-02-14T13:59:14Z  | physical     | failed: internet not invented yet",
        )
        failed_backup = formatted_output[3]
        self.assertEqual(failed_backup, "2000-02-14T14:09:43Z  | logical      | finished")
        inprogress_backup = formatted_output[4]
        self.assertEqual(inprogress_backup, "2023-02-14T17:06:38Z  | logical      | in progress")

        # case 2: running backup is not listed in error state
        with open("tests/unit/data/pbm_status.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        check_output.return_value = output_contents.encode("utf-8")
        formatted_output = self.harness.charm.backups._generate_backup_list_output()
        formatted_output = formatted_output.split("\n")
        header = formatted_output[0]
        self.assertEqual(header, "backup-id             | backup-type  | backup-status")
        divider = formatted_output[1]
        self.assertEqual(
            divider, "-" * len("backup-id             | backup-type  | backup-status")
        )
        eariest_backup = formatted_output[2]
        self.assertEqual(
            eariest_backup,
            "1900-02-14T13:59:14Z  | physical     | failed: internet not invented yet",
        )
        failed_backup = formatted_output[3]
        self.assertEqual(failed_backup, "2000-02-14T14:09:43Z  | logical      | finished")
        inprogress_backup = formatted_output[4]
        self.assertEqual(inprogress_backup, "2023-02-14T17:06:38Z  | logical      | in progress")

    @unittest.skip("Not implemented yet")
    def test_restore_without_rel(self):
        """Verifies no restores are attempted without s3 relation."""
        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}

        self.harness.charm.backups._on_restore_action(action_event)
        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.snap.SnapCache")
    def test_restore_syncing(self, snap, output):
        """Verifies restore is deferred if more time is needed to resync."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        output.return_value = b"Currently running:\n====\nResync op"

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)

        action_event.defer.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.snap.SnapCache")
    def test_restore_running_backup(self, snap, output):
        """Verifies restore is fails if another backup is already running."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        output.return_value = b"Currently running:\n====\nSnapshot backup"

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)

        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.snap.SnapCache")
    def test_restore_wrong_cred(self, snap, output):
        """Verifies restore is fails if the credentials are incorrect."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        output.side_effect = CalledProcessError(
            cmd="charmed-mongodb.pbm status", returncode=403, output=b"status code: 403"
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)
        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.MongoDBBackups._get_pbm_status")
    @patch("charm.snap.SnapCache")
    def test_restore_failed(self, snap, pbm_status, output):
        """Verifies restore is fails if the pbm command failed."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        pbm_status.return_value = ActiveStatus("")

        output.side_effect = CalledProcessError(
            cmd="charmed-mongodb.pbm backup", returncode=42, output=b"failed"
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)

        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    def test_remap_replicaset_no_backup(self, check_output):
        """Test verifies that no remapping is given if the backup_id doesn't exist."""
        with open("tests/unit/data/pbm_status.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        check_output.return_value = output_contents.encode("utf-8")
        remap = self.harness.charm.backups._remap_replicaset("this-id-doesnt-exist")
        self.assertEqual(remap, "")

    @unittest.skip("Not implemented yet")
    def test_remap_replicaset_no_remap_necessary(self, check_output):
        """Test verifies that no remapping is given if no remapping is necessary."""
        with open("tests/unit/data/pbm_status_error_remap.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        check_output.return_value = output_contents.encode("utf-8")

        # first case is that the backup is not in the error state
        remap = self.harness.charm.backups._remap_replicaset("2000-02-14T14:09:43Z")
        self.assertEqual(remap, "")

        # second case is that the backup has an error not related to remapping
        remap = self.harness.charm.backups._remap_replicaset("1900-02-14T13:59:14Z")
        self.assertEqual(remap, "")

        # third case is that the backup has two errors one related to remapping and another
        # related to something else
        remap = self.harness.charm.backups._remap_replicaset("2001-02-14T13:59:14Z")
        self.assertEqual(remap, "")

    @unittest.skip("Not implemented yet")
    def test_remap_replicaset_remap_necessary(self, check_output):
        """Test verifies that remapping is provided and correct when necessary."""
        with open("tests/unit/data/pbm_status_error_remap.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        check_output.return_value = output_contents.encode("utf-8")
        self.harness.charm.app.name = "current-app-name"

        # first case is that the backup is not in the error state
        remap = self.harness.charm.backups._remap_replicaset("2002-02-14T13:59:14Z")
        self.assertEqual(remap, "--replset-remapping current-app-name=old-cluster-name")

    @unittest.skip("Not implemented yet")
    @patch("charm.snap.SnapCache")
    def test_get_pbm_status_backup(self, snap, output):
        """Tests that when pbm running a backup that pbm is in maintenance state."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}
        output.return_value = b"Currently running:\n====\nSnapshot backup"
        self.assertTrue(
            isinstance(self.harness.charm.backups._get_pbm_status(), MaintenanceStatus)
        )

    @unittest.skip("Not implemented yet")
    def test_current_pbm_op(self):
        """Test if _current_pbm_op can identify the operation pbm is running."""
        action = self.harness.charm.backups._current_pbm_op(
            "nothing\nCurrently running:\n====\nexpected action"
        )
        self.assertEqual(action, "expected action")

        no_action = self.harness.charm.backups._current_pbm_op("pbm not started")
        self.assertEqual(no_action, "")

    @unittest.skip("Not implemented yet.")
    @patch("charm.snap.SnapCache")
    def test_backup_syncing(self, snap, output):
        """Verifies backup is deferred if more time is needed to resync."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {}
        output.return_value = b"Currently running:\n====\nResync op"

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)

        action_event.defer.assert_called()

    @unittest.skip("Not implemented yet.")
    @patch("charm.snap.SnapCache")
    def test_backup_running_backup(self, snap, output):
        """Verifies backup is fails if another backup is already running."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True
        snap.return_value = {"charmed-mongodb": mock_pbm_snap}

        action_event = mock.Mock()
        action_event.params = {}
        output.return_value = b"Currently running:\n====\nSnapshot backup"

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)

        action_event.fail.assert_called()

    @unittest.skip("Not implemented yet")
    @patch("charm.MongoDBCharm.run_pbm_command")
    def test_backup_wrong_cred(self, output):
        """Verifies backup is fails if the credentials are incorrect."""
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        action_event = mock.Mock()
        action_event.params = {}
        output.side_effect = ExecError(
            command=["/usr/bin/pbm config --set this_key=doesnt_exist"],
            exit_code=403,
            stdout="status code: 403",
            stderr="",
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)
        action_event.fail.assert_called()
