#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import shutil
import zipfile
from collections.abc import AsyncGenerator
from logging import getLogger
from pathlib import Path

import pytest
import pytest_asyncio
from pytest_operator.plugin import OpsTest

from ..backup_tests.helpers import get_leader_unit
from ..ha_tests.helpers import deploy_and_scale_application, get_direct_mongo_client
from ..helpers import MONGOS_PORT, mongodb_uri
from ..sharding_tests import writes_helpers
from ..sharding_tests.helpers import deploy_cluster_components, integrate_cluster
from .helpers import assert_successful_run_upgrade_sequence, get_workload_version

SHARD_ONE_DB_NAME = "shard_one_db"
SHARD_TWO_DB_NAME = "shard_two_db"
SHARD_ONE_COLL_NAME = "test_collection"
SHARD_TWO_COLL_NAME = "test_collection"
SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
CONFIG_SERVER_APP_NAME = "config-server"
CLUSTER_COMPONENTS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, CONFIG_SERVER_APP_NAME]
SHARD_APPS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME]
WRITE_APP = "application"

TIMEOUT = 15 * 60

logger = getLogger()


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


@pytest_asyncio.fixture(scope="module")
async def local_charm(ops_test: OpsTest) -> AsyncGenerator[Path]:
    """Builds the regular charm."""
    charm = await ops_test.build_charm(".")
    yield charm


@pytest_asyncio.fixture
def righty_upgrade_charm(local_charm, tmp_path: Path):
    right_charm = tmp_path / "right.charm"
    shutil.copy(local_charm, right_charm)
    workload_version = Path("workload_version").read_text().strip()
    charm_internal_version = Path("charm_internal_version").read_text().strip()

    [major, minor, patch] = workload_version.split(".")

    with zipfile.ZipFile(right_charm, mode="a") as charm_zip:
        charm_zip.writestr("workload_version", f"{major}.{int(minor)+1}.{patch}+testupgrade")
        charm_zip.writestr("charm_internal_version", f"{charm_internal_version}-upgraded")

    yield right_charm


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build deploy, and integrate, a sharded cluster."""
    await deploy_and_scale_application(ops_test)
    num_units_cluster_config = {
        CONFIG_SERVER_APP_NAME: 3,
        SHARD_ONE_APP_NAME: 3,
        SHARD_TWO_APP_NAME: 3,
    }
    await deploy_cluster_components(
        ops_test,
        num_units_cluster_config=num_units_cluster_config,
    )

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


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_pre_upgrade_check_success(ops_test: OpsTest) -> None:
    """Verify that the pre-refresh check succeeds in the happy path."""
    for sharding_component in CLUSTER_COMPONENTS:
        leader_unit = await get_leader_unit(ops_test, sharding_component)
        action = await leader_unit.run_action("pre-refresh-check")
        await action.wait()
        assert action.status == "completed", "pre-refresh-check failed, expected to succeed."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_upgrade_cluster(ops_test: OpsTest, righty_upgrade_charm, add_writes_to_shards):
    initial_version = Path("workload_version").read_text().strip()
    [major, minor, patch] = initial_version.split(".")
    new_version = f"{major}.{int(minor)+1}.{patch}+testupgrade"

    for sharding_component in CLUSTER_COMPONENTS:
        await assert_successful_run_upgrade_sequence(
            ops_test, sharding_component, new_charm=righty_upgrade_charm
        )

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS, status="active", idle_period=30, timeout=TIMEOUT
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
    logger.error(f"{actual_shard_one_writes = }, {actual_shard_two_writes = }")

    for sharding_component in CLUSTER_COMPONENTS:
        for unit in ops_test.model.applications[sharding_component].units:
            workload_version = await get_workload_version(ops_test, unit.name)
            assert workload_version == new_version
            assert initial_version != workload_version
