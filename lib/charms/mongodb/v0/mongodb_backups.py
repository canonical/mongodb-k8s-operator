# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class, we manage backup configurations and actions.

Specifically backups are handled with Percona Backup MongoDB (pbm).
A user for PBM is created when MongoDB is first started during the start phase.
This user is named "backup".
"""

import logging
import time
import traceback
from typing import Dict

from charms.data_platform_libs.v0.s3 import CredentialsChangedEvent, S3Requirer
from ops.framework import Object
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    ModelError,
    StatusBase,
    WaitingStatus,
)
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
PBM_CONFIG_FILE_PATH = "/etc/pbm_config.yaml"
PBM_PATH = "/usr/bin/pbm"
PBM_STATUS_CMD = [PBM_PATH, "status", "-o", "json"]


class ResyncError(Exception):
    """Raised when pbm is resyncing configurations and is not ready to be used."""


class SetPBMConfigError(Exception):
    """Raised when pbm cannot configure a given option."""


class PBMBusyError(Exception):
    """Raised when PBM is busy and cannot run another operation."""


class PBMError(Exception):
    """Raised when PBM encounters an error."""

    def __init__(self, retcode, stdout, stderr):
        self.retcode = retcode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            f"Command exited with code {self.retcode}. StdOut: {self.stdout}. Stderr: {self.stderr}"
        )


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
        if not self.charm.db_initialised:
            logger.error("Cannot set PBM configurations, MongoDB has not yet started.")
            event.defer()
            return

        try:
            self.charm.get_backup_service()
        except ModelError as e:
            trace_str = traceback.format_exc()
            logger.error(
                f"Cannot set PBM configurations, pbm-agent service not found. {e}. {trace_str}"
            )
            event.defer()
        except RuntimeError as e:
            trace_str = traceback.format_exc()
            logger.error(
                f"Cannot set PBM configurations. Failed to get pbm serivice. {e}. {trace_str}"
            )
            self.charm.unit.status = BlockedStatus("couldn't configure s3 backup options.")

        try:
            self._set_config_options()
            self._resync_config_options()
        except ResyncError as e:
            trace_str = traceback.format_exc()
            self.charm.unit.status = WaitingStatus("waiting to sync s3 configurations.")
            event.defer()
            logger.error(f"Sync-ing configurations needs more time: {e}. {trace_str}")
            return
        except PBMBusyError as e:
            trace_str = traceback.format_exc()
            self.charm.unit.status = WaitingStatus("waiting to sync s3 configurations.")
            logger.error(
                f"Cannot update configs while PBM is running, must wait for PBM action to finish: {e} {trace_str}"
            )
            event.defer()
            return
        except Exception as e:
            trace_str = traceback.format_exc()
            logger.error(f"Syncing configurations failed: {e}, {trace_str}")

        self.charm.unit.status = self._get_pbm_status()

    def _on_create_backup_action(self, event) -> None:
        if self.model.get_relation(S3_RELATION) is None:
            event.fail("Relation with s3-integrator charm missing, cannot create backup.")
            return

        # only leader can create backups. This prevents multiple backups from being attempted at
        # once.
        if not self.charm.unit.is_leader():
            event.fail("The action can be run only on leader unit.")
            return

        # cannot create backup if pbm is not ready. This could be due to: resyncing, incompatible,
        # options, incorrect credentials, or already creating a backup
        pbm_status = self._get_pbm_status()
        self.charm.unit.status = pbm_status
        if isinstance(pbm_status, MaintenanceStatus):
            event.fail(
                "Can only create one backup at a time, please wait for current backup to finish."
            )
            return
        if isinstance(pbm_status, WaitingStatus):
            event.defer()
            logger.debug(
                "Sync-ing configurations needs more time, must wait before creating a backup."
            )
            return
        if isinstance(pbm_status, BlockedStatus):
            event.fail(f"Cannot create backup {pbm_status.message}.")
            return

        # TODO create backup

    def _get_pbm_status(self) -> StatusBase:
        """Retrieve pbm status."""
        try:
            self.charm.get_backup_service()
        except ModelError as e:
            return BlockedStatus(f"pbm-agent service not found. {e}")

        try:
            pbm_status = self.charm.run_pbm_command(PBM_STATUS_CMD)
            return self._process_pbm_status(pbm_status)
        except PBMError as e:
            logger.error(f"Failed to get pbm status: {e}")
            return BlockedStatus(self._process_pbm_error(e))
        except Exception as e:
            # pbm pipes a return code of 1, but its output shows the true error code so it is
            # necessary to parse the output
            logger.error(f"Failed to get pbm status: {e}")
            return BlockedStatus("PBM error")

    def _process_pbm_status(self, pbm_status: str) -> StatusBase:
        # pbm is running resync operation
        if "Resync" in self._current_pbm_op(pbm_status):
            return WaitingStatus("waiting to sync s3 configurations.")

        # no operations are currently running with pbm
        if "(none)" in self._current_pbm_op(pbm_status):
            return ActiveStatus("")

        if "Snapshot backup" in self._current_pbm_op(pbm_status):
            return MaintenanceStatus("backup started/running")

        if "Snapshot restore" in self._current_pbm_op(pbm_status):
            return MaintenanceStatus("restore started/running")

        return ActiveStatus("")

    def _on_list_backups_action(self, event) -> None:
        if self.model.get_relation(S3_RELATION) is None:
            event.fail("Relation with s3-integrator charm missing, cannot list backups.")
            return

        # cannot list backups if pbm is resyncing, or has incompatible options or incorrect
        # credentials
        pbm_status = self._get_pbm_status()
        self.charm.unit.status = pbm_status
        if isinstance(pbm_status, WaitingStatus):
            event.defer()
            logger.debug(
                "Sync-ing configurations needs more time, must wait before listing backups."
            )
            return
        if isinstance(pbm_status, BlockedStatus):
            event.fail(f"Cannot list backups: {pbm_status.message}.")
            return

        # TODO list backups

    def _on_restore_action(self, event) -> None:
        if self.model.get_relation(S3_RELATION) is None:
            event.fail("Relation with s3-integrator charm missing, cannot restore from a backup.")
            return

        backup_id = event.params.get("backup-id")
        if not backup_id:
            event.fail("Missing backup-id to restore")
            return

        # only leader can restore backups. This prevents multiple restores from being attempted at
        # once.
        if not self.charm.unit.is_leader():
            event.fail("The action can be run only on leader unit.")
            return

        # cannot restore backup if pbm is not ready. This could be due to: resyncing, incompatible,
        # options, incorrect credentials, creating a backup, or already performing a restore.
        try:
            pbm_status = self._get_pbm_status()
            # TOD check status
            self.charm.unit.status = pbm_status
            if isinstance(pbm_status, MaintenanceStatus):
                event.fail("Please wait for current backup/restore to finish.")
                return
            if isinstance(pbm_status, WaitingStatus):
                event.defer()
                logger.debug(
                    "Sync-ing configurations needs more time, must wait before restoring."
                )
                return
            if isinstance(pbm_status, BlockedStatus):
                event.fail(f"Cannot restore backup {pbm_status.message}.")
                return
        except PBMError as e:
            logger.error(f"Failed to get pbm status: {e.stdout} {e.stderr}")
            self.charm.unit.status = BlockedStatus("PBM error")

        # TODO restore backup

    # BEGIN: helper functions
    def _set_config_options(self):
        """Applying given configurations with pbm."""
        # Setting empty pbm
        container = self.charm.get_container()
        container.push(
            PBM_CONFIG_FILE_PATH,
            "# this file is to be left empty. Changes in this file will be ignored.\n",
            make_dirs=True,
            permissions=0o400,
        )
        try:
            self.charm.run_pbm_command([PBM_PATH, "config", "--file", PBM_CONFIG_FILE_PATH])
        except PBMError as e:
            logger.error(f"Failed to set pbm config file. {e}")
            self.charm.unit.status = BlockedStatus(self._process_pbm_error(e))
            return

        # the pbm tool can only set one configuration at a time.
        for pbm_key, pbm_value in self._get_pbm_configs().items():
            config_cmd = f"{PBM_PATH} config --set {pbm_key}={pbm_value}".split(" ")
            try:
                self.charm.run_pbm_command(config_cmd)
            except PBMError as e:
                logger.error(f"Failed to set {pbm_key} with pbm config command. {e}")
                self.charm.unit.status = BlockedStatus(self._process_pbm_error(e))
                return

    def _process_pbm_error(self, e: PBMError) -> str:
        message = "couldn't configure s3 backup option"
        if "status code: 403" in e.stdout:
            message = "s3 credentials are incorrect."
        if "status code: 404" in e.stdout:
            message = "s3 configurations are incompatible."
        if "status code: 301" in e.stdout:
            message = "s3 configurations are incompatible."
        return message

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
        container = self.charm.get_container()
        container.restart("pbm-agent")

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
                    container.restart("pbm-agent")
                    raise PBMBusyError

        # wait for re-sync and update charm status based on pbm syncing status. Need to wait for
        # 2 seconds for pbm_agent to receive the resync command before verifying.
        # subprocess.check_output("charmed-mongodb.pbm config --force-resync", shell=True)
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
                    pbm_status = self.charm.run_pbm_command(PBM_STATUS_CMD)
                    if "Resync" in self._current_pbm_op(pbm_status):
                        # since this process takes several minutes we should let the user know
                        # immediately.
                        self.charm.unit.status = WaitingStatus(
                            "waiting to sync s3 configurations."
                        )
                        raise ResyncError
                except PBMError as e:
                    self.charm.unit.status = BlockedStatus(self._process_pbm_error(e))

    def _current_pbm_op(self, pbm_status: str) -> str:
        """Parses pbm status for the operation that pbm is running."""
        pbm_status_lines = pbm_status.splitlines()
        for i in range(0, len(pbm_status_lines)):
            line = pbm_status_lines[i]

            # operation is two lines after the line "Currently running:"
            if line == "Currently running:":
                return pbm_status_lines[i + 2]

        return ""
