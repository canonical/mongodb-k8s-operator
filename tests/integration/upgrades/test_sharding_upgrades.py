#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from pytest_operator.plugin import OpsTest

from ..ha_tests.helpers import (
    deploy_and_scale_application,
    get_direct_mongo_client,
    isolate_instance_from_cluster,
    remove_instance_isolation,
    wait_until_unit_in_status,
)
from ..helpers import MONGOS_PORT, mongodb_uri
from ..sharding_tests import writes_helpers
from ..sharding_tests.helpers import deploy_cluster_components, integrate_cluster
from .helpers import assert_successful_run_upgrade_sequence, backup_helpers

SHARD_ONE_DB_NAME = "shard_one_db"
SHARD_ONE_COLL_NAME = "test_collection"
SHARD_TWO_DB_NAME = "shard_two_db"
SHARD_TWO_COLL_NAME = "test_collection"
SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
CONFIG_SERVER_APP_NAME = "config-server"
CLUSTER_COMPONENTS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, CONFIG_SERVER_APP_NAME]
SHARD_APPS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME]
WRITE_APP = "application"
TIMEOUT = 15 * 60


@pytest.mark.skip()
@pytest.fixture()
async def add_writes_to_shards(ops_test: OpsTest):
    """Adds writes to each shard before test starts and clears writes at the end of the test."""
    application_unit = ops_test.model.applications[WRITE_APP].units[0]

    start_writes_action = await application_unit.run_action(
        "start-continuous-writes",
        **{"db-name": SHARD_ONE_DB_NAME, "coll-name": SHARD_ONE_COLL_NAME},
    )
    await start_writes_action.wait()

    start_writes_action = await application_unit.run_action(
        "start-continuous-writes",
        **{"db-name": SHARD_TWO_DB_NAME, "coll-name": SHARD_TWO_COLL_NAME},
    )
    await start_writes_action.wait()

    # # move continuous writes so they are present on each shard
    mongos_client = await get_direct_mongo_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )
    mongos_client.admin.command("movePrimary", SHARD_ONE_DB_NAME, to=SHARD_ONE_APP_NAME)
    mongos_client.admin.command("movePrimary", SHARD_TWO_DB_NAME, to=SHARD_TWO_APP_NAME)

    yield
    clear_writes_action = await application_unit.run_action(
        "clear-continuous-writes",
        **{"db-name": SHARD_ONE_DB_NAME, "coll-name": SHARD_ONE_COLL_NAME},
    )
    await clear_writes_action.wait()

    clear_writes_action = await application_unit.run_action(
        "clear-continuous-writes",
        **{"db-name": SHARD_TWO_DB_NAME, "coll-name": SHARD_TWO_APP_NAME},
    )
    await clear_writes_action.wait()


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build deploy, and integrate, a sharded cluster."""
    await deploy_and_scale_application(ops_test)

    await deploy_cluster_components(ops_test)

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        raise_on_blocked=False,
        raise_on_error=False,
    )

    await integrate_cluster(ops_test)
    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
    )

    # configure write app to use mongos uri
    mongos_uri = await mongodb_uri(ops_test, app_name=CONFIG_SERVER_APP_NAME, port=MONGOS_PORT)
    await ops_test.model.applications[WRITE_APP].set_config({"mongos-uri": mongos_uri})


@pytest.mark.skip()
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_upgrade(ops_test: OpsTest, add_writes_to_shards) -> None:
    """Verify that the sharded cluster can be safely upgraded without losing writes."""
    new_charm = await ops_test.build_charm(".")

    for sharding_component in CLUSTER_COMPONENTS:
        await assert_successful_run_upgrade_sequence(
            ops_test, sharding_component, new_charm=new_charm
        )

    application_unit = ops_test.model.applications[WRITE_APP].units[0]
    stop_writes_action = await application_unit.run_action(
        "stop-continuous-writes",
        **{"db-name": SHARD_ONE_DB_NAME, "coll-name": SHARD_ONE_COLL_NAME},
    )
    await stop_writes_action.wait()
    shard_one_expected_writes = int(stop_writes_action.results["writes"])
    stop_writes_action = await application_unit.run_action(
        "stop-continuous-writes",
        **{"db-name": SHARD_TWO_DB_NAME, "coll-name": SHARD_TWO_COLL_NAME},
    )
    await stop_writes_action.wait()
    shard_two_total_expected_writes = int(stop_writes_action.results["writes"])

    actual_shard_one_writes = await writes_helpers.count_shard_writes(
        ops_test,
        config_server_name=CONFIG_SERVER_APP_NAME,
        db_name=SHARD_ONE_DB_NAME,
    )
    actual_shard_two_writes = await writes_helpers.count_shard_writes(
        ops_test,
        config_server_name=CONFIG_SERVER_APP_NAME,
        db_name=SHARD_TWO_DB_NAME,
    )

    assert (
        actual_shard_one_writes == shard_one_expected_writes
    ), "missed writes during upgrade procedure."
    assert (
        actual_shard_two_writes == shard_two_total_expected_writes
    ), "missed writes during upgrade procedure."


@pytest.mark.skip()
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_pre_upgrade_check_success(ops_test: OpsTest) -> None:
    """Verify that the pre-upgrade check succeeds in the happy path."""
    for sharding_component in CLUSTER_COMPONENTS:
        leader_unit = await backup_helpers.get_leader_unit(ops_test, sharding_component)
        action = await leader_unit.run_action("pre-upgrade-check")
        await action.wait()
        assert action.status == "completed", "pre-upgrade-check failed, expected to succeed."


@pytest.mark.skip()
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_pre_upgrade_check_failure(ops_test: OpsTest, chaos_mesh) -> None:
    """Verify that the pre-upgrade check fails if there is a problem with one of the shards."""
    leader_unit = await backup_helpers.get_leader_unit(ops_test, SHARD_TWO_APP_NAME)

    non_leader_unit = None
    for unit in ops_test.model.applications[SHARD_TWO_APP_NAME].units:
        if unit != leader_unit:
            non_leader_unit = unit
            break

    isolate_instance_from_cluster(ops_test, non_leader_unit.name)
    await wait_until_unit_in_status(
        ops_test, non_leader_unit, leader_unit, "(not reachable/healthy)"
    )

    for sharding_component in CLUSTER_COMPONENTS:
        leader_unit = await backup_helpers.get_leader_unit(ops_test, sharding_component)
        action = await leader_unit.run_action("pre-upgrade-check")
        await action.wait()
        assert action.status == "completed", "pre-upgrade-check failed, expected to succeed."

    # restore network after test
    remove_instance_isolation(ops_test)
    await ops_test.model.wait_for_idle(
        apps=[SHARD_TWO_APP_NAME], status="active", timeout=1000, idle_period=30
    )
