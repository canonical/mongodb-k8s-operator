#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import time
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from pytest_operator.plugin import OpsTest

from tests.integration.ha_tests.helpers import (
    MONGODB_CONTAINER_NAME,
    TEST_COLLECTION,
    TEST_DB,
    count_primaries,
    db_step_down,
    deploy_and_scale_application,
    deploy_and_scale_mongodb,
    fetch_replica_set_members,
    find_focal_in_cluster,
    find_unit,
    get_application_name,
    get_mongo_client,
    get_other_mongodb_direct_client,
    get_process_pid,
    get_replica_set_primary,
    get_units_hostnames,
    insert_focal_to_cluster,
    kubectl_delete,
    mongod_ready,
    relate_mongodb_and_application,
    retrieve_entries,
    scale_application,
    send_signal_to_pod_container_process,
    set_log_level,
    verify_writes,
)
from tests.integration.helpers import APP_NAME

ANOTHER_DATABASE_APP_NAME = "another-database-a"
MONGOD_PROCESS_NAME = "mongod"
MEDIAN_REELECTION_TIME = 12


@pytest.fixture
def cmd_mongodb_charm(request):
    """Fixture to optionally pass a prebuilt charm to deploy."""
    return request.config.getoption("--mongodb_charm")


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


@pytest_asyncio.fixture
async def change_logging(ops_test: OpsTest):
    """Increases and resets election logging verbosity."""
    await set_log_level(ops_test, 5, "replication.election")

    yield

    await set_log_level(ops_test, -1, "replication.election")


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, cmd_mongodb_charm) -> None:
    """Build and deploy three units of MongoDB and one test unit."""
    # it is possible for users to provide their own cluster for HA testing. Hence check if there
    # is a pre-existing cluster.
    mongodb_application_name = await get_application_name(ops_test, APP_NAME)
    if not mongodb_application_name:
        mongodb_application_name = await deploy_and_scale_mongodb(
            ops_test, charm_path=cmd_mongodb_charm
        )
    application_name = await get_application_name(ops_test, "application")
    if not application_name:
        application_name = await deploy_and_scale_application(ops_test)

    await relate_mongodb_and_application(ops_test, mongodb_application_name, application_name)


@pytest.mark.abort_on_fail
async def test_scale_up_capablities(ops_test: OpsTest, continuous_writes) -> None:
    """Tests juju add-unit functionality.

    Verifies that when a new unit is added to the MongoDB application that it is added to the
    MongoDB replica set configuration.
    """
    await insert_focal_to_cluster(ops_test)

    # add units and wait for idle
    app = await get_application_name(ops_test, APP_NAME)
    await scale_application(ops_test, app, len(ops_test.model.applications[app].units) + 2)

    # grab unit hosts
    hostnames = await get_units_hostnames(ops_test)

    # connect to replica set uri and get replica set members
    member_hosts = await fetch_replica_set_members(ops_test)

    # verify that the replica set members have the correct units
    assert set(member_hosts) == set(hostnames), "all members not running under the same replset"

    # verify that the no writes were skipped
    await find_focal_in_cluster(ops_test)
    await verify_writes(ops_test)


@pytest.mark.abort_on_fail
async def test_scale_down_capablities(ops_test: OpsTest, continuous_writes) -> None:
    """Tests clusters behavior when scaling down a minority and removing a primary replica."""
    await insert_focal_to_cluster(ops_test)

    app = await get_application_name(ops_test, APP_NAME)
    minority_count = int(len(ops_test.model.applications[app].units) / 2)
    expected_units = len(ops_test.model.applications[app].units) - minority_count

    # find leader unit
    leader_unit = await find_unit(ops_test, leader=True)

    # verify that we have a leader
    assert leader_unit is not None, "No unit is leader"

    # Force delete the leader and scale down
    await kubectl_delete(ops_test, leader_unit, False)
    await scale_application(ops_test, app, expected_units)

    # grab unit hosts
    hostnames = await get_units_hostnames(ops_test)

    # check that the replica set with the remaining units has a primary
    primary = await get_replica_set_primary(ops_test)

    # verify that the primary is not None
    assert primary is not None, "replica set has no primary"

    # check that the primary is one of the remaining units
    assert (
        f"{primary.replace('/', '-')}.mongodb-k8s-endpoints" in hostnames
    ), "replica set primary is not one of the available units"

    # verify that the configuration of mongodb no longer has the deleted ip
    member_hosts = await fetch_replica_set_members(ops_test)

    # verify that the replica set members have the correct units
    assert set(member_hosts) == set(hostnames), "mongod config contains deleted units"

    # verify that the no writes were skipped
    await find_focal_in_cluster(ops_test)
    await verify_writes(ops_test)


async def test_replication_across_members(ops_test: OpsTest, continuous_writes) -> None:
    """Check consistency, ie write to primary, read data from secondaries."""
    # first find primary, write to primary, then read from each unit
    await insert_focal_to_cluster(ops_test)

    # verify that the no writes were skipped
    await find_focal_in_cluster(ops_test)
    await verify_writes(ops_test)


async def test_unique_cluster_dbs(ops_test: OpsTest, continuous_writes, cmd_mongodb_charm) -> None:
    """Verify unique clusters do not share DBs."""
    # first find primary, write to primary,
    await insert_focal_to_cluster(ops_test)

    # deploy new cluster
    if ANOTHER_DATABASE_APP_NAME not in ops_test.model.applications:
        await deploy_and_scale_mongodb(ops_test, ANOTHER_DATABASE_APP_NAME, 1, cmd_mongodb_charm)

    # write data to new cluster
    with await get_other_mongodb_direct_client(ops_test, ANOTHER_DATABASE_APP_NAME) as client:
        db = client["new-db"]
        test_collection = db["test_ubuntu_collection"]
        test_collection.insert_one({"release_name": "Jammy Jelly", "version": 22.04, "LTS": False})

        cluster_1_entries = retrieve_entries(
            client,
            db_name="new-db",
            collection_name="test_ubuntu_collection",
            query_field="release_name",
        )
    with await get_mongo_client(ops_test) as client:
        cluster_2_entries = retrieve_entries(
            client,
            db_name="new-db",
            collection_name="test_ubuntu_collection",
            query_field="release_name",
        )

    common_entries = cluster_2_entries.intersection(cluster_1_entries)
    assert len(common_entries) == 0, "Writes from one cluster are replicated to another cluster."

    # verify that the no writes were skipped
    await find_focal_in_cluster(ops_test)
    await verify_writes(ops_test)


async def test_kill_db_process(ops_test: OpsTest, continuous_writes):
    # locate primary unit
    hostnames = await get_units_hostnames(ops_test)
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

    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    with await get_mongo_client(ops_test, excluded=[primary]) as client:
        writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
        time.sleep(5)
        more_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert more_writes > writes, "writes not continuing to DB"

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

    # verify all units are running under the same replset
    member_hosts = await fetch_replica_set_members(ops_test)
    assert set(member_hosts) == set(hostnames), "all members not running under the same replset"

    # verify there is only one primary after killing old primary
    assert (
        await count_primaries(ops_test) == 1
    ), "there are more than one primary in the replica set."

    # verify that no writes to the db were missed
    await verify_writes(ops_test)


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
    with await get_mongo_client(ops_test, excluded=[primary]) as client:
        writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
        time.sleep(5)
        more_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})

    # un-freeze the old primary
    await send_signal_to_pod_container_process(
        ops_test,
        primary,
        MONGODB_CONTAINER_NAME,
        MONGOD_PROCESS_NAME,
        "SIGCONT",
    )

    # check this after un-freezing the old primary so that if this check fails we still "turned
    # back on" the mongod process
    assert more_writes > writes, "writes not continuing to DB"

    # verify that db service got restarted and is ready
    old_primary_unit = int(primary.split("/")[1])
    assert await mongod_ready(ops_test, old_primary_unit)

    # verify all units are running under the same replset
    member_hosts = await fetch_replica_set_members(ops_test)
    assert set(member_hosts) == set(hostnames), "all members not running under the same replset"

    # verify there is only one primary after un-freezing old primary
    assert (
        await count_primaries(ops_test) == 1
    ), "there are more than one primary in the replica set."

    # verify that the old primary does not "reclaim" primary status after un-freezing old primary
    new_primary = await get_replica_set_primary(ops_test)
    assert new_primary != primary, "un-frozen primary should be secondary."

    # verify that no writes were missed.
    await verify_writes(ops_test)


async def test_restart_db_process(ops_test, continuous_writes, change_logging):
    # locate primary unit
    old_primary = await get_replica_set_primary(ops_test)

    # send SIGTERM
    sig_term_time = datetime.now(timezone.utc)
    await send_signal_to_pod_container_process(
        ops_test,
        old_primary,
        MONGODB_CONTAINER_NAME,
        MONGOD_PROCESS_NAME,
        "SIGTERM",
    )
    # verify that a stepdown was performed on restart. SIGTERM should send a graceful restart and
    # send a replica step down signal. Pipes k8s logs output to see if any of the pods received a
    # stepdown request. Must be done early otherwise continuous writes may flood the logs
    await db_step_down(ops_test, sig_term_time)

    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    with await get_mongo_client(ops_test, excluded=[old_primary]) as client:
        writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
        time.sleep(5)
        more_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
    assert more_writes > writes, "writes not continuing to DB"

    # verify that db service got restarted and is ready
    old_primary_unit = int(old_primary.split("/")[1])
    assert await mongod_ready(ops_test, old_primary_unit)

    # verify that a new primary gets elected (ie old primary is secondary)
    new_primary = await get_replica_set_primary(ops_test)
    assert new_primary != old_primary

    # verify there is only one primary after killing old primary
    assert (
        await count_primaries(ops_test) == 1
    ), "there are more than one primary in the replica set."

    # verify that old primary is up to date.
    await verify_writes(ops_test)
