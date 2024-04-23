#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import secrets
import string
import time
from pathlib import Path

import pytest
import pytest_asyncio
import yaml
from pytest_operator.plugin import OpsTest
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

from ..ha_tests import helpers as ha_helpers
from . import helpers

S3_APP_NAME = "s3-integrator"
TIMEOUT = 15 * 60
ENDPOINT = "s3-credentials"
NEW_CLUSTER = "new-mongodb"
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
DATABASE_APP_NAME = METADATA["name"]

logger = logging.getLogger(__name__)


# TODO this should be refactored to remove duplication
@pytest_asyncio.fixture
async def continuous_writes_to_db(ops_test: OpsTest):
    """Continuously writes to DB for the duration of the test."""
    application_name = await ha_helpers.get_application_name(ops_test, "application")

    application_unit = ops_test.model.applications[application_name].units[0]

    clear_writes_action = await application_unit.run_action("clear-continuous-writes")
    await clear_writes_action.wait()

    start_writes_action = await application_unit.run_action("start-continuous-writes")
    await start_writes_action.wait()

    yield

    clear_writes_action = await application_unit.run_action("clear-continuous-writes")
    await clear_writes_action.wait()


@pytest_asyncio.fixture
async def add_writes_to_db(ops_test: OpsTest):
    """Adds writes to DB before test starts and clears writes at the end of the test."""
    application_name = await ha_helpers.get_application_name(ops_test, "application")

    application_unit = ops_test.model.applications[application_name].units[0]

    clear_writes_action = await application_unit.run_action("clear-continuous-writes")
    await clear_writes_action.wait()

    start_writes_action = await application_unit.run_action("start-continuous-writes")
    await start_writes_action.wait()

    time.sleep(20)

    stop_writes_action = await application_unit.run_action("stop-continuous-writes")
    await stop_writes_action.wait()

    yield

    clear_writes_action = await application_unit.run_action("clear-continuous-writes")
    await clear_writes_action.wait()


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, local_application_charm) -> None:
    """Build and deploy one unit of MongoDB."""
    # it is possible for users to provide their own cluster for testing. Hence check if there
    # is a pre-existing cluster.
    db_app_name = await ha_helpers.get_application_name(ops_test, DATABASE_APP_NAME)
    if db_app_name:
        return

    async with ops_test.fast_forward():
        my_charm = await ops_test.build_charm(".")
        resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}
        await ops_test.model.deploy(my_charm, num_units=3, resources=resources, series="jammy")
        await ops_test.model.wait_for_idle(apps=[DATABASE_APP_NAME], status="active", timeout=2000)

    # deploy the s3 integrator charm
    await ops_test.model.deploy(S3_APP_NAME, channel="edge")

    # test application
    application_name = await ha_helpers.get_application_name(ops_test, "application")
    if not application_name:
        application_name = await ha_helpers.deploy_and_scale_local_application(
            ops_test, local_application_charm
        )

    db_app_name = await ha_helpers.get_application_name(ops_test, DATABASE_APP_NAME)
    await ha_helpers.relate_mongodb_and_application(ops_test, db_app_name, application_name)

    await ops_test.model.wait_for_idle()


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_blocked_incorrect_creds(ops_test: OpsTest) -> None:
    """Verifies that the charm goes into blocked status when s3 creds are incorrect."""
    db_app_name = await helpers.app_name(ops_test)

    # set incorrect s3 credentials
    s3_integrator_unit = ops_test.model.applications[S3_APP_NAME].units[0]
    parameters = {"access-key": "user", "secret-key": "doesnt-exist"}
    action = await s3_integrator_unit.run_action(action_name="sync-s3-credentials", **parameters)
    await action.wait()

    # relate after s3 becomes active add and wait for relation
    await ops_test.model.wait_for_idle(apps=[S3_APP_NAME], status="active")
    await ops_test.model.add_relation(S3_APP_NAME, db_app_name)
    await ops_test.model.block_until(
        lambda: helpers.is_relation_joined(ops_test, ENDPOINT, ENDPOINT) is True,
        timeout=TIMEOUT,
    )

    # verify that Charmed MongoDB is blocked and reports incorrect credentials
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[S3_APP_NAME], status="active"),
        ops_test.model.wait_for_idle(apps=[db_app_name], status="blocked", idle_period=20),
    )
    db_unit = ops_test.model.applications[db_app_name].units[0]

    assert db_unit.workload_status_message == "s3 credentials are incorrect."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_blocked_incorrect_conf(ops_test: OpsTest) -> None:
    """Verifies that the charm goes into blocked status when s3 config options are incorrect."""
    db_app_name = await helpers.app_name(ops_test)

    # set correct AWS credentials for s3 storage but incorrect configs
    await helpers.set_credentials(ops_test, cloud="AWS")

    # wait for both applications to be idle with the correct statuses
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[S3_APP_NAME], status="active"),
        ops_test.model.wait_for_idle(apps=[db_app_name], status="blocked", idle_period=20),
    )
    db_unit = ops_test.model.applications[db_app_name].units[0]
    assert db_unit.workload_status_message == "s3 configurations are incompatible."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_ready_correct_conf(ops_test: OpsTest) -> None:
    """Verifies charm goes into active status when s3 config and creds options are correct."""
    db_app_name = await helpers.app_name(ops_test)
    choices = string.ascii_letters + string.digits
    unique_path = "".join([secrets.choice(choices) for _ in range(4)])
    configuration_parameters = {
        "bucket": "data-charms-testing",
        "path": f"mongodb-vm/test-{unique_path}",
        "endpoint": "https://s3.amazonaws.com",
        "region": "us-east-1",
    }

    # apply new configuration options
    await ops_test.model.applications[S3_APP_NAME].set_config(configuration_parameters)

    # after applying correct config options and creds the applications should both be active
    await ops_test.model.wait_for_idle(apps=[S3_APP_NAME], status="active", timeout=TIMEOUT)
    await ops_test.model.wait_for_idle(
        apps=[db_app_name], status="active", timeout=TIMEOUT, idle_period=60
    )


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_create_and_list_backups(ops_test: OpsTest) -> None:
    db_unit = await helpers.get_leader_unit(ops_test)

    # verify backup list works
    action = await db_unit.run_action(action_name="list-backups")
    list_result = await action.wait()
    backups = list_result.results["backups"]

    # verify backup is started
    action = await db_unit.run_action(action_name="create-backup")
    backup_result = await action.wait()
    assert "backup started" in backup_result.results["backup-status"], "backup didn't start"

    # verify backup is present in the list of backups
    # the action `create-backup` only confirms that the command was sent to the `pbm`. Creating a
    # backup can take a lot of time so this function returns once the command was successfully
    # sent to pbm. Therefore we should retry listing the backup several times
    try:
        for attempt in Retrying(stop=stop_after_delay(20), wait=wait_fixed(3)):
            with attempt:
                backups = await helpers.count_logical_backups(db_unit)
                assert backups == 1
    except RetryError:
        assert backups == 1, "Backup not created."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_multi_backup(ops_test: OpsTest, continuous_writes_to_db) -> None:
    """With writes in the DB test creating a backup while another one is running.

    Note that before creating the second backup we change the bucket and change the s3 storage
    from AWS to GCP. This test verifies that the first backup in AWS is made, the second backup
    in GCP is made, and that before the second backup is made that pbm correctly resyncs.
    """
    db_app_name = await helpers.app_name(ops_test)
    db_unit = await helpers.get_leader_unit(ops_test)

    # create first backup once ready
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    action = await db_unit.run_action(action_name="create-backup")
    first_backup = await action.wait()
    assert first_backup.status == "completed", "First backup not started."

    # while first backup is running change access key, secret keys, and bucket name
    # for GCP
    await helpers.set_credentials(ops_test, cloud="GCP")

    # change to GCP configs and wait for PBM to resync
    configuration_parameters = {
        "bucket": "data-charms-testing",
        "endpoint": "https://storage.googleapis.com",
        "region": "",
    }
    await ops_test.model.applications[S3_APP_NAME].set_config(configuration_parameters)

    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    # create a backup as soon as possible. might not be immediately possible since only one backup
    # can happen at a time.
    try:
        for attempt in Retrying(stop=stop_after_delay(40), wait=wait_fixed(5)):
            with attempt:
                action = await db_unit.run_action(action_name="create-backup")
                second_backup = await action.wait()
                assert second_backup.status == "completed"
    except RetryError:
        assert second_backup.status == "completed", "Second backup not started."

    # the action `create-backup` only confirms that the command was sent to the `pbm`. Creating a
    # backup can take a lot of time so this function returns once the command was successfully
    # sent to pbm. Therefore before checking, wait for Charmed MongoDB to finish creating the
    # backup
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    # verify that backups was made in GCP bucket
    try:
        for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(5)):
            with attempt:
                backups = await helpers.count_logical_backups(db_unit)
                assert backups == 1, "Backup not created in bucket on GCP."
    except RetryError:
        assert backups == 1, "Backup not created in first bucket on GCP."

    # set AWS credentials, set configs for s3 storage, and wait to resync
    await helpers.set_credentials(ops_test, cloud="AWS")
    configuration_parameters = {
        "bucket": "data-charms-testing",
        "region": "us-east-1",
        "endpoint": "https://s3.amazonaws.com",
    }
    await ops_test.model.applications[S3_APP_NAME].set_config(configuration_parameters)
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    # verify that backups was made on the AWS bucket
    try:
        for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(5)):
            with attempt:
                backups = await helpers.count_logical_backups(db_unit)
                assert backups == 2, "Backup not created in bucket on AWS."
    except RetryError:
        assert backups == 2, "Backup not created in bucket on AWS."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_restore(ops_test: OpsTest, continuous_writes_to_db) -> None:
    """Simple backup tests that verifies that writes are correctly restored."""
    # count total writes
    number_writes = await ha_helpers.get_total_writes(ops_test)
    assert number_writes > 0, "no writes to backup"

    # create a backup in the AWS bucket
    db_app_name = await helpers.app_name(ops_test)
    db_unit = await helpers.get_leader_unit(ops_test)
    prev_backups = await helpers.count_logical_backups(db_unit)
    action = await db_unit.run_action(action_name="create-backup")
    first_backup = await action.wait()
    assert first_backup.status == "completed", "First backup not started."

    # verify that backup was made on the bucket
    try:
        for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(5)):
            with attempt:
                backups = await helpers.count_logical_backups(db_unit)
                assert backups == prev_backups + 1, "Backup not created."
    except RetryError:
        assert backups == prev_backups + 1, "Backup not created."

    # add writes to be cleared after restoring the backup. Note these are written to the same
    # collection that was backed up.
    application_name = await ha_helpers.get_application_name(ops_test, "application")
    application_unit = ops_test.model.applications[application_name].units[0]
    start_writes_action = await application_unit.run_action("start-continuous-writes")
    await start_writes_action.wait()
    time.sleep(20)
    new_number_of_writes = await ha_helpers.get_total_writes(ops_test)
    assert new_number_of_writes > number_writes, "No writes to be cleared after restoring."

    # find most recent backup id and restore
    action = await db_unit.run_action(action_name="list-backups")
    list_result = await action.wait()
    list_result = list_result.results["backups"]
    most_recent_backup = list_result.split("\n")[-1]
    backup_id = most_recent_backup.split()[0]
    action = await db_unit.run_action(action_name="restore", **{"backup-id": backup_id})
    restore = await action.wait()
    assert restore.results["restore-status"] == "restore started", "restore not successful"

    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    number_writes_restored = number_writes  # initialize extra write count
    # verify all writes are present
    try:
        for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(20)):
            with attempt:
                number_writes_restored = await ha_helpers.count_writes(ops_test)
                assert number_writes == number_writes_restored, "writes not correctly restored"
    except RetryError:
        assert number_writes == number_writes_restored, "writes not correctly restored"


# TODO remove unstable mark once juju issue with secrets is resolved
@pytest.mark.group(1)
@pytest.mark.unstable
@pytest.mark.parametrize("cloud_provider", ["AWS", "GCP"])
async def test_restore_new_cluster(ops_test: OpsTest, continuous_writes_to_db, cloud_provider):
    # configure test for the cloud provider
    db_app_name = await helpers.app_name(ops_test)
    leader_unit = await helpers.get_leader_unit(ops_test, db_app_name)
    await helpers.set_credentials(ops_test, cloud=cloud_provider)
    if cloud_provider == "AWS":
        configuration_parameters = {
            "bucket": "data-charms-testing",
            "region": "us-east-1",
            "endpoint": "https://s3.amazonaws.com",
        }
    else:
        configuration_parameters = {
            "bucket": "data-charms-testing",
            "endpoint": "https://storage.googleapis.com",
            "region": "",
        }

    await ops_test.model.applications[S3_APP_NAME].set_config(configuration_parameters)
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[S3_APP_NAME], status="active"),
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    # sleep to allow for writes to be made
    time.sleep(30)
    writes_in_old_cluster = await ha_helpers.get_total_writes(ops_test)
    assert writes_in_old_cluster > 0, "old cluster has no writes."

    # create a backup
    action = await leader_unit.run_action(action_name="create-backup")
    latest_backup = await action.wait()
    assert latest_backup.status == "completed", "Backup not started."

    # save old password, since after restoring we will need this password to authenticate.

    action = await leader_unit.run_action("get-password", **{"username": "operator"})
    action = await action.wait()
    old_password = action.results["password"]

    # TODO remove this workaround once issue with juju secrets is fixed
    NEW_CLUSTER = get_new_cluster_name(cloud_provider)  # noqa: N806

    # deploy a new cluster with a different name
    db_charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}
    await ops_test.model.deploy(
        db_charm, num_units=3, resources=resources, application_name=NEW_CLUSTER
    )

    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[NEW_CLUSTER], status="active", idle_period=20),
    )

    leader_unit = await helpers.get_leader_unit(ops_test, db_app_name=NEW_CLUSTER)
    action = await leader_unit.run_action("set-password", **{"password": old_password})
    action = await action.wait()
    assert action.status == "completed"

    # relate to s3 - s3 has the necessary configurations
    await ops_test.model.add_relation(S3_APP_NAME, NEW_CLUSTER)
    await ops_test.model.block_until(
        lambda: helpers.is_relation_joined(ops_test, ENDPOINT, ENDPOINT) is True,
        timeout=TIMEOUT,
    )

    # wait for new cluster to sync
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[NEW_CLUSTER], status="active", idle_period=20),
    )

    # verify that the listed backups from the old cluster are not listed as failed.
    assert (
        await helpers.count_failed_backups(leader_unit) == 0
    ), "Backups from old cluster are listed as failed"

    # find most recent backup id and restore
    action = await leader_unit.run_action(action_name="list-backups")
    list_result = await action.wait()
    list_result = list_result.results["backups"]
    most_recent_backup = list_result.split("\n")[-1]
    backup_id = most_recent_backup.split()[0]
    action = await leader_unit.run_action(action_name="restore", **{"backup-id": backup_id})
    restore = await action.wait()
    assert restore.results["restore-status"] == "restore started", "restore not successful"

    # initialize with old values
    writes_in_new_cluster = writes_in_old_cluster
    # verify all writes are present
    try:
        for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(20)):
            with attempt:
                writes_in_new_cluster = await ha_helpers.count_writes(ops_test, NEW_CLUSTER)
                assert (
                    writes_in_new_cluster == writes_in_old_cluster
                ), "new cluster writes do not match old cluster writes after restore"
    except RetryError:
        assert (
            writes_in_new_cluster == writes_in_old_cluster
        ), "new cluster writes do not match old cluster writes after restore"

    # TODO there is an issue with on stop and secrets that need to be resolved before
    # we can cleanup the new cluster, otherwise the test will fail.

    # await helpers.destroy_cluster(ops_test, cluster_name=NEW_CLUSTER)


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_update_backup_password(ops_test: OpsTest) -> None:
    """Verifies that after changing the backup password the pbm tool is updated and functional."""
    db_app_name = await helpers.app_name(ops_test)
    leader_unit = await helpers.get_leader_unit(ops_test)

    # wait for charm to be idle before setting password
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    parameters = {"username": "backup"}
    action = await leader_unit.run_action("set-password", **parameters)
    action = await action.wait()
    assert action.status == "completed", "failed to set backup password"

    # wait for charm to be idle after setting password
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[db_app_name], status="active", idle_period=20),
    )

    # verify we still have connection to pbm via creating a backup
    action = await leader_unit.run_action(action_name="create-backup")
    backup_result = await action.wait()
    assert "backup started" in backup_result.results["backup-status"], "backup didn't start"


# TODO remove this workaround once issue with juju secrets is fixed
def get_new_cluster_name(cloud_provider: str) -> str:
    """Generates a new cluster name."""
    return f"{NEW_CLUSTER}-{cloud_provider.lower()}"
