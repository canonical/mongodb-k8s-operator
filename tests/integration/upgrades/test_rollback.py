#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import shutil
import time
import zipfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
import tenacity
from pytest_operator.plugin import OpsTest

from ..helpers import APP_NAME, RESOURCES, get_juju_status, get_leader_id
from .helpers import get_workload_version

logger = logging.getLogger(__name__)

UPGRADE_TIMEOUT = 15 * 60


@pytest_asyncio.fixture
async def local_charm(ops_test: OpsTest) -> AsyncGenerator[Path]:
    """Builds the regular charm."""
    charm = await ops_test.build_charm(".")
    yield charm


@pytest_asyncio.fixture
def faulty_upgrade_charm(local_charm, tmp_path: Path):
    fault_charm = tmp_path / "fault_charm.charm"
    shutil.copy(local_charm, fault_charm)
    workload_version = Path("workload_version").read_text().strip()

    [major, minor, patch] = workload_version.split(".")

    with zipfile.ZipFile(fault_charm, mode="a") as charm_zip:
        charm_zip.writestr("workload_version", f"{int(major) -1}.{minor}.{patch}+testrollback")

    yield fault_charm


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, local_charm: Path):
    """Build and deploy a sharded cluster."""
    await ops_test.model.deploy(
        local_charm,
        resources=RESOURCES,
        application_name=APP_NAME,
        num_units=3,
        series="jammy",
        trust=True,
    )
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", raise_on_blocked=True, timeout=1000, raise_on_error=False
    )


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_rollback(ops_test: OpsTest, local_charm, faulty_upgrade_charm) -> None:
    mongodb_application = ops_test.model.applications[APP_NAME]

    initial_version = Path("workload_version").read_text().strip()

    await mongodb_application.refresh(path=faulty_upgrade_charm)
    logger.info("Wait for refresh to fail")

    for attempt in tenacity.Retrying(
        reraise=True,
        stop=tenacity.stop_after_delay(UPGRADE_TIMEOUT),
        wait=tenacity.wait_fixed(10),
    ):
        with attempt:
            assert "Refresh incompatible" in get_juju_status(
                ops_test.model.name, APP_NAME
            ), "Not indicating charm incompatible"

    logger.info("Re-refresh the charm")
    await mongodb_application.refresh(path=local_charm)
    # sleep to ensure that active status from before re-refresh does not affect below check
    time.sleep(15)
    await ops_test.model.block_until(
        lambda: all(unit.workload_status == "active" for unit in mongodb_application.units)
        and all(unit.agent_status == "idle" for unit in mongodb_application.units)
    )

    logger.info("Running resume-refresh on the leader unit")
    leader_id = await get_leader_id(ops_test, APP_NAME)
    leader_unit = ops_test.model.units.get(f"{APP_NAME}/{leader_id}")
    action = await leader_unit.run_action("resume-refresh")
    await action.wait()

    logger.info("Wait for the charm to be rolled back")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
        idle_period=30,
    )

    for unit in mongodb_application.units:
        workload_version = await get_workload_version(ops_test, unit.name)
        assert workload_version == initial_version
