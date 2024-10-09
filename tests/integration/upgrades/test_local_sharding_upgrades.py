#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import shutil
import zipfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
from pytest_operator.plugin import OpsTest

from ..backup_tests.helpers import get_leader_unit
from ..sharding_tests.helpers import deploy_cluster_components, integrate_cluster
from .helpers import assert_successful_run_upgrade_sequence, get_workload_version

SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
CONFIG_SERVER_APP_NAME = "config-server"
CLUSTER_COMPONENTS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, CONFIG_SERVER_APP_NAME]

TIMEOUT = 15 * 60


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
    num_units_cluster_config = {
        CONFIG_SERVER_APP_NAME: 1,
        SHARD_ONE_APP_NAME: 1,
        SHARD_TWO_APP_NAME: 1,
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
async def test_upgrade_config_server(ops_test: OpsTest, righty_upgrade_charm):
    initial_version = Path("workload_version").read_text().strip()
    [major, minor, patch] = initial_version.split(".")
    new_version = f"{major}.{int(minor)+1}.{patch}+testupgrade"

    for sharding_component in CLUSTER_COMPONENTS:
        await assert_successful_run_upgrade_sequence(
            ops_test, sharding_component, new_charm=righty_upgrade_charm
        )

    await ops_test.model.wait_for_idle(apps=CLUSTER_COMPONENTS, status="active", idle_period=30)
    for sharding_component in CLUSTER_COMPONENTS:
        for unit in ops_test.model.applications[sharding_component].units:
            workload_version = await get_workload_version(ops_test, unit.name)
            assert workload_version == new_version
            assert initial_version != workload_version
