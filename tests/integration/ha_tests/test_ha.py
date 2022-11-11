#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import time

import pytest
import pytest_asyncio
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest

from tests.integration.ha_tests.helpers import (
    deploy_and_scale_application,
    deploy_and_scale_mongodb,
    get_application_name,
    get_process_pid,
    host_to_unit,
    mongod_ready,
    relate_mongodb_and_application,
    send_signal_to_pod_container_process,
)
from tests.integration.helpers import APP_NAME, mongodb_uri, primary_host, run_mongo_op

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
    mongodb_application_name = await deploy_and_scale_mongodb(ops_test)
    application_name = await deploy_and_scale_application(ops_test)

    await relate_mongodb_and_application(ops_test, mongodb_application_name, application_name)


async def test_kill_db_process(ops_test: OpsTest, continuous_writes):
    # locate primary unit
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    # get the primary host from the rs_status response
    primary = host_to_unit(primary_host(rs_status.data))

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
    mongodb_name = await get_application_name(ops_test, APP_NAME)
    units = ops_test.model.applications[mongodb_name].units
    for i in range(len(units)):
        if units[i].name != primary:
            client = MongoClient(await mongodb_uri(ops_test, [i]), directConnection=True)
            break

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

    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    # verify that a new primary gets elected (ie old primary is secondary)
    new_primary = host_to_unit(primary_host(rs_status.data))
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
    client = MongoClient(await mongodb_uri(ops_test, [old_primary_unit]), directConnection=True)
    total_old_primary = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert (
        total_old_primary == total_expected_writes
    ), "secondary not up to date with the cluster after restarting."
