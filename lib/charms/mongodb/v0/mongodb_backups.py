# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class, we manage backup configurations and actions.

Specifically backups are handled with Percona Backup MongoDB (pbm).
A user for PBM is created when MongoDB is first started during the start phase.
This user is named "backup".
"""

import json
import logging
import re
import subprocess
import time
from typing import Dict, List

from charms.data_platform_libs.v0.s3 import CredentialsChangedEvent, S3Requirer
from charms.mongodb.v0.helpers import (
    current_pbm_op,
    process_pbm_error,
    process_pbm_status,
)
from charms.operator_libs_linux.v1 import snap
from ops.framework import Object
from ops.model import (
    BlockedStatus,
    MaintenanceStatus,
    ModelError,
    StatusBase,
    WaitingStatus,
)
from ops.pebble import ExecError
from tenacity import (
    Retrying,
    before_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

# The unique Charmhub library identifier, never change it
LIBID = "9f2b91c6128d48d6ba22724bf365da3b"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)

S3_PBM_OPTION_MAP = {
    "region": "storage.s3.region",
    "bucket": "storage.s3.bucket",
    "path": "storage.s3.prefix",
    "access-key": "storage.s3.credentials.access-key-id",
    "secret-key": "storage.s3.credentials.secret-access-key",
    "endpoint": "storage.s3.endpointUrl",
    "storage-class": "storage.s3.storageClass",
}
S3_RELATION = "s3-credentials"
REMAPPING_PATTERN = r"\ABackup doesn't match current cluster topology - it has different replica set names. Extra shards in the backup will cause this, for a simple example. The extra/unknown replica set names found in the backup are: ([^,\s]+)([.] Backup has no data for the config server or sole replicaset)?\Z"
PBM_STATUS_CMD = ["status", "-o", "json"]
MONGODB_SNAP_DATA_DIR = "/var/snap/charmed-mongodb/current"
RESTORE_MAX_ATTEMPTS = 5
RESTORE_ATTEMPT_COOLDOWN = 15


class ResyncError(Exception):
    """Raised when pbm is resyncing configurations and is not ready to be used."""


class SetPBMConfigError(Exception):
    """Raised when pbm cannot configure a given option."""


class PBMBusyError(Exception):
    """Raised when PBM is busy and cannot run another operation."""


class RestoreError(Exception):
    """Raised when backup operation is failed."""


def _restore_retry_before_sleep(retry_state) -> None:
    logger.error(
        f"Attempt {retry_state.attempt_number} failed. {RESTORE_MAX_ATTEMPTS - retry_state.attempt_number} attempts left. Retrying after {RESTORE_ATTEMPT_COOLDOWN} seconds."
    ),


def _restore_stop_condition(retry_state) -> bool:
    if isinstance(retry_state.outcome.exception(), RestoreError):
        return True
    return retry_state.attempt_number >= RESTORE_MAX_ATTEMPTS


class MongoDBBackups(Object):
    """Manages MongoDB backups."""

    def __init__(self, charm, substrate):
        """Manager of MongoDB client relations."""
        super().__init__(charm, "client-relations")
        self.charm = charm
        self.substrate = substrate

        # s3 relation handles the config options for s3 backups
        self.s3_client = S3Requirer(self.charm, S3_RELATION)
        self.framework.observe(
            self.s3_client.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(self.charm.on.create_backup_action, self._on_create_backup_action)
        self.framework.observe(self.charm.on.list_backups_action, self._on_list_backups_action)
        self.framework.observe(self.charm.on.restore_action, self._on_restore_action)

    def _on_s3_credential_changed(self, event: CredentialsChangedEvent):
        """Sets pbm credentials, resyncs if necessary and reports config errors."""
        # handling PBM configurations requires that MongoDB is running and the pbm snap is
        # installed.
        action = "configure-pbm"
        if not self.charm.db_initialised:
            self._defer_action_with_info_log(
                event, action, "Set PBM credentials, MongoDB not ready."
            )
            return

        try:
            # TODO VM charm should implement this  method
            self.charm.get_backup_service()
        except ModelError:
            self._defer_action_with_info_log(
                event, action, "Set PBM configurations, pbm-agent service not found."
            )
            return

        self._configure_pbm_options(event)

    def _on_create_backup_action(self, event) -> None:
        action = "backup"
        if self.model.get_relation(S3_RELATION) is None:
            self._fail_action_with_error_log(
                event,
                action,
                "Relation with s3-integrator charm missing, cannot create backup.",
            )
            return

        # only leader can create backups. This prevents multiple backups from being attempted at
        # once.
        if not self.charm.unit.is_leader():
            self._fail_action_with_error_log(
                event, action, "The action can be run only on leader unit."
            )
            return

        # cannot create backup if pbm is not ready. This could be due to: resyncing, incompatible,
        # options, incorrect credentials, or already creating a backup
        pbm_status = self._get_pbm_status()
        self.charm.unit.status = pbm_status

        if isinstance(pbm_status, MaintenanceStatus):
            self._fail_action_with_error_log(
                event,
                action,
                "Can only create one backup at a time, please wait for current backup to finish.",
            )
            return

        if isinstance(pbm_status, WaitingStatus):
            self._defer_action_with_info_log(
                event,
                action,
                "Sync-ing configurations needs more time, must wait before creating a backup.",
            )
            return

        if isinstance(pbm_status, BlockedStatus):
            self._fail_action_with_error_log(event, action, pbm_status.message)
            return

        try:
            self.charm.unit.status = MaintenanceStatus("backup started/running")
            self.charm.run_pbm_command(["backup"])
            self._success_action_with_info_log(event, action, {"backup-status": "backup started"})
        except (subprocess.CalledProcessError, ExecError, Exception) as e:
            self._fail_action_with_error_log(event, action, str(e))
            return

    def _on_list_backups_action(self, event) -> None:
        action = "list-backups"
        if self.model.get_relation(S3_RELATION) is None:
            self._fail_action_with_error_log(
                event,
                action,
                "Relation with s3-integrator charm missing, cannot list backups.",
            )
            return

        # cannot list backups if pbm is resyncing, or has incompatible options or incorrect
        # credentials
        pbm_status = self._get_pbm_status()
        self.charm.unit.status = pbm_status

        if isinstance(pbm_status, WaitingStatus):
            self._defer_action_with_info_log(
                event,
                action,
                "Sync-ing configurations needs more time, must wait before listing backups.",
            )
            return

        if isinstance(pbm_status, BlockedStatus):
            self._fail_action_with_error_log(event, action, pbm_status.message)
            return

        try:
            formatted_list = self._generate_backup_list_output()
            self._success_action_with_info_log(event, action, {"backups": formatted_list})
        except (subprocess.CalledProcessError, ExecError) as e:
            self._fail_action_with_error_log(event, action, str(e))
            return

    def _on_restore_action(self, event) -> None:
        action = "restore"
        if self.model.get_relation(S3_RELATION) is None:
            self._fail_action_with_error_log(
                event,
                action,
                "Relation with s3-integrator charm missing, cannot restore from a backup.",
            )
            return

        backup_id = event.params.get("backup-id")
        if not backup_id:
            self._fail_action_with_error_log(event, action, "Missing backup-id to restore")
            return

        # only leader can restore backups. This prevents multiple restores from being attempted at
        # once.
        if not self.charm.unit.is_leader():
            self._fail_action_with_error_log(
                event, action, "The action can be run only on leader unit."
            )
            return

        # cannot restore backup if pbm is not ready. This could be due to: resyncing, incompatible,
        # options, incorrect credentials, creating a backup, or already performing a restore.
        pbm_status = self._get_pbm_status()
        self.charm.unit.status = pbm_status
        if isinstance(pbm_status, MaintenanceStatus):
            self._fail_action_with_error_log(
                event, action, "Please wait for current backup/restore to finish."
            )
            return

        if isinstance(pbm_status, WaitingStatus):
            self._defer_action_with_info_log(
                event,
                action,
                "Sync-ing configurations needs more time, must wait before restoring.",
            )
            return

        if isinstance(pbm_status, BlockedStatus):
            self._fail_action_with_error_log(
                event, action, f"Cannot restore backup {pbm_status.message}."
            )
            return

        # sometimes when we are trying to restore pmb can be resyncing, so we need to retry
        try:
            self.charm.unit.status = MaintenanceStatus("restore started/running")
            self._try_to_restore(backup_id)
            self._success_action_with_info_log(
                event, action, {"restore-status": "restore started"}
            )
            logger.info("Restore succeeded.")
        except ResyncError:
            raise
        except RestoreError as restore_error:
            self._fail_action_with_error_log(event, action, str(restore_error))

    # BEGIN: helper functions

    def _configure_pbm_options(self, event) -> None:
        action = "configure-pbm"
        try:
            self._set_config_options()
            self._resync_config_options()
        except SetPBMConfigError:
            self.charm.unit.status = BlockedStatus("couldn't configure s3 backup options.")
            return
        except snap.SnapError as e:
            logger.error("An exception occurred when starting pbm agent, error: %s.", str(e))
            self.charm.unit.status = BlockedStatus("couldn't start pbm")
            return
        except ResyncError:
            self.charm.unit.status = WaitingStatus("waiting to sync s3 configurations.")
            self._defer_action_with_info_log(
                event, action, "Sync-ing configurations needs more time."
            )
            return
        except PBMBusyError:
            self.charm.unit.status = WaitingStatus("waiting to sync s3 configurations.")
            self._defer_action_with_info_log(
                event,
                action,
                "Cannot update configs while PBM is running, must wait for PBM action to finish.",
            ),
            return
        except ExecError as e:
            self.charm.unit.status = BlockedStatus(process_pbm_error(e.stdout))
            return
        except subprocess.CalledProcessError as e:
            logger.error("Syncing configurations failed: %s", str(e))

        self.charm.unit.status = self._get_pbm_status()

    def _set_config_options(self):
        """Applying given configurations with pbm."""
        # TODO VM charm should implement this method
        self.charm.set_pbm_config_file()

        # the pbm tool can only set one configuration at a time.
        for pbm_key, pbm_value in self._get_pbm_configs().items():
            try:
                config_cmd = ["config", "--set", f"{pbm_key}={pbm_value}"]
                # TODO VM charm should implement this method
                self.charm.run_pbm_command(config_cmd)
            except (subprocess.CalledProcessError, ExecError):
                logger.error(
                    "Failed to configure the PBM snap option: %s",
                    pbm_key,
                )
                raise SetPBMConfigError

    def _get_pbm_configs(self) -> Dict:
        """Returns a dictionary of desired PBM configurations."""
        pbm_configs = {"storage.type": "s3"}
        credentials = self.s3_client.get_s3_connection_info()
        for s3_option, s3_value in credentials.items():
            if s3_option not in S3_PBM_OPTION_MAP:
                continue

            pbm_configs[S3_PBM_OPTION_MAP[s3_option]] = s3_value
        return pbm_configs

    def _resync_config_options(self):
        """Attempts to sync pbm config options and sets status in case of failure."""
        # TODO VM charm should implement this method
        self.charm.start_backup_service()

        # pbm has a flakely resync and it is necessary to wait for no actions to be running before
        # resync-ing. See: https://jira.percona.com/browse/PBM-1038
        for attempt in Retrying(
            stop=stop_after_attempt(20),
            wait=wait_fixed(5),
            reraise=True,
        ):
            with attempt:
                pbm_status = self._get_pbm_status()
                # wait for backup/restore to finish
                if isinstance(pbm_status, (MaintenanceStatus)):
                    raise PBMBusyError

                # if a resync is running restart the service
                if isinstance(pbm_status, (WaitingStatus)):
                    # TODO VM charm should implement this method
                    self.charm.restart_backup_service()
                    raise PBMBusyError

        # wait for re-sync and update charm status based on pbm syncing status. Need to wait for
        # 2 seconds for pbm_agent to receive the resync command before verifying.
        self.charm.run_pbm_command(["config", "--force-resync"])
        time.sleep(2)
        self._wait_pbm_status()

    @retry(
        stop=stop_after_attempt(20),
        reraise=True,
        retry=retry_if_exception_type(ResyncError),
        before=before_log(logger, logging.DEBUG),
    )
    def _wait_pbm_status(self) -> None:
        """Wait for pbm_agent to resolve errors and return the status of pbm.

        The pbm status is set by the pbm_agent daemon which needs time to both resync and resolve
        errors in configurations. Resync-ing is a longer process and should take around 5 minutes.
        Configuration errors generally occur when the configurations change and pbm_agent is
        updating, this is generally quick and should take <15s. If errors are not resolved in 15s
        it means there is an incorrect configuration which will require user intervention.

        Retrying for resync is handled by decorator, retrying for configuration errors is handled
        within this function.
        """
        # on occasion it takes the pbm_agent daemon time to update its configs, meaning that it
        # will error for incorrect configurations for <15s before resolving itself.

        for attempt in Retrying(
            stop=stop_after_attempt(3),
            wait=wait_fixed(5),
            reraise=True,
        ):
            with attempt:
                try:
                    # TODO VM charm should implement this method
                    pbm_status = self.charm.run_pbm_command(PBM_STATUS_CMD)

                    if "Resync" in current_pbm_op(pbm_status):
                        # since this process takes several minutes we should let the user know
                        # immediately.
                        self.charm.unit.status = WaitingStatus(
                            "waiting to sync s3 configurations."
                        )
                        raise ResyncError
                except ExecError as e:
                    self.charm.unit.status = BlockedStatus(process_pbm_error(e.stdout))

    def _get_pbm_status(self) -> StatusBase:
        """Retrieve pbm status."""
        try:
            # TODO VM charm should implement this method
            self.charm.get_backup_service()
        except ModelError:
            return WaitingStatus("waiting for pbm to start")

        try:
            # TODO VM charm should implement this method
            pbm_status = self.charm.run_pbm_command(PBM_STATUS_CMD)
            return process_pbm_status(pbm_status)
        except ExecError as e:
            logger.error("Failed to get pbm status.")
            return BlockedStatus(process_pbm_error(e.stdout))
        except subprocess.CalledProcessError as e:
            # pbm pipes a return code of 1, but its output shows the true error code so it is
            # necessary to parse the output
            return BlockedStatus(process_pbm_error(e.output))
        except Exception as e:
            # pbm pipes a return code of 1, but its output shows the true error code so it is
            # necessary to parse the output
            logger.error(f"Failed to get pbm status: {e}")
            return BlockedStatus("PBM error")

    def _generate_backup_list_output(self) -> str:
        """Generates a list of backups in a formatted table.

        List contains successful, failed, and in progress backups in order of ascending time.

        Raises ExecError if pbm command fails.
        """
        backup_list = []
        pbm_status = self.charm.run_pbm_command(["status", "--out=json"])
        # processes finished and failed backups
        pbm_status = json.loads(pbm_status)
        backups = pbm_status["backups"]["snapshot"] or []
        for backup in backups:
            backup_status = "finished"
            if backup["status"] == "error":
                # backups from a different cluster have an error status, but they should show as
                # finished
                if self._backup_from_different_cluster(backup.get("error", "")):
                    backup_status = "finished"
                else:
                    # display reason for failure if available
                    backup_status = "failed: " + backup.get("error", "N/A")
            if backup["status"] not in ["error", "done"]:
                backup_status = "in progress"
            backup_list.append((backup["name"], backup["type"], backup_status))

        # process in progress backups
        running_backup = pbm_status["running"]
        if running_backup.get("type", None) == "backup":
            # backups are sorted in reverse order
            last_reported_backup = backup_list[0]
            # pbm will occasionally report backups that are currently running as failed, so it is
            # necessary to correct the backup list in this case.
            if last_reported_backup[0] == running_backup["name"]:
                backup_list[0] = (last_reported_backup[0], last_reported_backup[1], "in progress")
            else:
                backup_list.append((running_backup["name"], "logical", "in progress"))

        # sort by time and return formatted output
        return self._format_backup_list(sorted(backup_list, key=lambda pair: pair[0]))

    def _format_backup_list(self, backup_list: List[str]) -> str:
        """Formats provided list of backups as a table."""
        backups = ["{:<21s} | {:<12s} | {:s}".format("backup-id", "backup-type", "backup-status")]

        backups.append("-" * len(backups[0]))
        for backup_id, backup_type, backup_status in backup_list:
            backups.append(
                "{:<21s} | {:<12s} | {:s}".format(backup_id, backup_type, backup_status)
            )

        return "\n".join(backups)

    def _backup_from_different_cluster(self, backup_status: str) -> bool:
        """Returns if a given backup was made on a different cluster."""
        return re.search(REMAPPING_PATTERN, backup_status) is not None

    def _try_to_restore(self, backup_id: str) -> None:
        for attempt in Retrying(
            stop=_restore_stop_condition,
            wait=wait_fixed(RESTORE_ATTEMPT_COOLDOWN),
            reraise=True,
            before_sleep=_restore_retry_before_sleep,
        ):
            with attempt:
                try:
                    remapping_args = self._remap_replicaset(backup_id)
                    self.charm.run_pbm_restore_command(backup_id, remapping_args)
                except (subprocess.CalledProcessError, ExecError) as e:
                    if type(e) == subprocess.CalledProcessError:
                        error_message = e.output.decode("utf-8")
                    else:
                        error_message = str(e.stderr)
                    fail_message = f"Restore failed: {str(e)}"

                    if "Resync" in error_message:
                        raise ResyncError

                    if f"backup '{backup_id}' not found" in error_message:
                        fail_message = f"Restore failed: Backup id '{backup_id}' does not exist in list of backups, please check list-backups for the available backup_ids."

                    raise RestoreError(fail_message)

    def _remap_replicaset(self, backup_id: str) -> str:
        """Returns options for remapping a replica set during a cluster migration restore.

        Args:
            backup_id: str of the backup to check for remapping

        Raises: CalledProcessError
        """
        pbm_status = self.charm.run_pbm_command(PBM_STATUS_CMD)
        pbm_status = json.loads(pbm_status)

        # grab the error status from the backup if present
        backups = pbm_status["backups"]["snapshot"] or []
        backup_status = ""
        for backup in backups:
            if not backup_id == backup["name"]:
                continue

            backup_status = backup.get("error", "")
            break

        if not self._backup_from_different_cluster(backup_status):
            return ""

        # TODO in the future when we support conf servers and shards this will need to be more
        # comprehensive.
        old_cluster_name = re.search(REMAPPING_PATTERN, backup_status).group(1)
        current_cluster_name = self.charm.app.name
        logger.debug(
            "Replica set remapping is necessary for restore, old cluster name: %s ; new cluster name: %s",
            old_cluster_name,
            current_cluster_name,
        )
        return f"--replset-remapping {current_cluster_name}={old_cluster_name}"

    def _fail_action_with_error_log(self, event, action: str, message: str) -> None:
        logger.error("%s failed: %s", action.capitalize(), message)
        event.fail(message)

    def _defer_action_with_info_log(self, event, action: str, message: str) -> None:
        logger.info("Deferring %s: %s", action, message)
        event.defer()

    def _success_action_with_info_log(self, event, action: str, results: Dict[str, str]) -> None:
        logger.info("%s completed successfully", action.capitalize())
        event.set_results(results)
