#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import time

import pytest
import pytest_asyncio
from pytest_operator.plugin import OpsTest

from tests.integration.ha_tests.helpers import (
    count_primaries,
    deploy_and_scale_application,
    deploy_and_scale_mongodb,
    fetch_replica_set_members,
    get_application_name,
    get_mongo_client,
    get_process_pid,
    get_replica_set_primary,
    get_units_hostnames,
    mongod_ready,
    relate_mongodb_and_application,
    send_signal_to_pod_container_process,
)
from tests.integration.helpers import APP_NAME

MONGODB_CONTAINER_NAME = "mongod"
MONGOD_PROCESS_NAME = "mongod"
MEDIAN_REELECTION_TIME = 12
TEST_DB = "continuous_writes_database"
TEST_COLLECTION = "test_collection"


@pytest_asyncio.fixture
async def continuous_writes(ops_test: OpsTest) -> None:
    """Starts continuous writes to the MySQL cluster for a test and clear the writes at the end."""
    application_name = await get_application_name(ops_test, "application")

    application_unit = ops_test.model.applications[application_name].units[0]

    clear_writes_action = await application_unit.run_action("clear-continuous-writes")
    await clear_writes_action.wait()

    start_writes_action = await application_unit.run_action("start-continuous-writes")
    await start_writes_action.wait()

    yield

    clear_writes_action = await application_unit.run_action("clear-continuous-writes")
    await clear_writes_action.wait()


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy three units of MongoDB and one test unit."""
    # it is possible for users to provide their own cluster for HA testing. Hence check if there
    # is a pre-existing cluster.
    mongodb_application_name = await get_application_name(ops_test, APP_NAME)
    if not mongodb_application_name:
        mongodb_application_name = await deploy_and_scale_mongodb(ops_test)
    application_name = await get_application_name(ops_test, "application")
    if not application_name:
        application_name = await deploy_and_scale_application(ops_test)

    await relate_mongodb_and_application(ops_test, mongodb_application_name, application_name)


async def test_kill_db_process(ops_test: OpsTest, continuous_writes):
    # locate primary unit
    primary = await get_replica_set_primary(ops_test)

    mongodb_pid = await get_process_pid(
        ops_test, primary, MONGODB_CONTAINER_NAME, MONGOD_PROCESS_NAME
    )

    await send_signal_to_pod_container_process(
        ops_test,
        primary,
        MONGODB_CONTAINER_NAME,
        MONGOD_PROCESS_NAME,
        "SIGKILL",
    )
    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    client = await get_mongo_client(ops_test, excluded=[primary])

    writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
    time.sleep(5)
    more_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert more_writes > writes, "writes not continuing to DB"

    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # verify that db service got restarted and is ready
    old_primary_unit = int(primary.split("/")[1])
    assert await mongod_ready(ops_test, old_primary_unit)
    new_mongodb_pid = await get_process_pid(
        ops_test, primary, MONGODB_CONTAINER_NAME, MONGOD_PROCESS_NAME
    )
    assert (
        mongodb_pid != new_mongodb_pid
    ), "The mongodb process id is the same after sending it a SIGKILL"

    # verify that a new primary gets elected (ie old primary is secondary)
    new_primary = await get_replica_set_primary(ops_test)
    assert (
        primary != new_primary
    ), "The mongodb primary has not been reelected after sending a SIGKILL"

    # verify that no writes to the db were missed
    application_name = await get_application_name(ops_test, "application")
    application_unit = ops_test.model.applications[application_name].units[0]
    stop_writes_action = await application_unit.run_action("stop-continuous-writes")
    await stop_writes_action.wait()
    total_expected_writes = int(stop_writes_action.results["writes"])
    actual_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert total_expected_writes == actual_writes, "writes to the db were missed."

    # verify that old primary is up to date.
    client = await get_mongo_client(ops_test, exact=primary)
    total_old_primary = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert (
        total_old_primary == total_expected_writes
    ), "secondary not up to date with the cluster after restarting."


async def test_freeze_db_process(ops_test, continuous_writes):
    # locate primary unit
    hostnames = await get_units_hostnames(ops_test)
    primary = await get_replica_set_primary(ops_test)

    await send_signal_to_pod_container_process(
        ops_test,
        primary,
        MONGODB_CONTAINER_NAME,
        MONGOD_PROCESS_NAME,
        "SIGSTOP",
    )

    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # verify that a new primary gets elected
    new_primary = await get_replica_set_primary(ops_test)
    assert (
        primary != new_primary
    ), "The mongodb primary has not been reelected after sending a SIGSTOP"

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    client = await get_mongo_client(ops_test, excluded=[primary])

    writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
    time.sleep(5)
    more_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})

    # check this after un-freezing the old primary so that if this check fails we still "turned
    # back on" the mongod process
    assert more_writes > writes, "writes not continuing to DB"

    # un-freeze the old primary
    await send_signal_to_pod_container_process(
        ops_test,
        primary,
        MONGODB_CONTAINER_NAME,
        MONGOD_PROCESS_NAME,
        "SIGCONT",
    )

    # verify that db service got restarted and is ready
    old_primary_unit = int(primary.split("/")[1])
    assert await mongod_ready(ops_test, old_primary_unit)

    # verify all units are running under the same replset
    member_ips = await fetch_replica_set_members(ops_test)
    assert set(member_ips) == set(hostnames), "all members not running under the same replset"

    # verify there is only one primary after un-freezing old primary
    assert (
        await count_primaries(ops_test) == 1
    ), "there are more than one primary in the replica set."

    # verify that the old primary does not "reclaim" primary status after un-freezing old primary
    new_primary = await get_replica_set_primary(ops_test)
    assert new_primary != primary, "un-frozen primary should be secondary."

    # verify that no writes were missed.
    application_name = await get_application_name(ops_test, "application")
    application_unit = ops_test.model.applications[application_name].units[0]
    stop_writes_action = await application_unit.run_action("stop-continuous-writes")
    await stop_writes_action.wait()
    total_expected_writes = int(stop_writes_action.results["writes"])
    actual_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert actual_writes == total_expected_writes, "db writes missing."

    # verify that old primary is up to date.
    client = await get_mongo_client(ops_test, exact=primary)
    total_old_primary = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert (
        total_old_primary == total_expected_writes
    ), "secondary not up to date with the cluster after restarting."
