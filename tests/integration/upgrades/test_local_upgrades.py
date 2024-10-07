#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import shutil
import zipfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
import tenacity
from pytest_operator.plugin import OpsTest

from ..helpers import APP_NAME, METADATA, get_juju_status, get_leader_id
from .helpers import get_workload_version

logger = logging.getLogger(__name__)

UPGRADE_TIMEOUT = 15 * 60
SMALL_TIMEOUT = 5 * 60


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

    [major, minor, patch] = workload_version.split(".")

    with zipfile.ZipFile(right_charm, mode="a") as charm_zip:
        charm_zip.writestr("workload_version", f"{major}.{int(minor)+1}.{patch}+testupgrade")

    yield right_charm


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, local_charm: Path):
    """Build and deploy a sharded cluster."""
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}
    await ops_test.model.deploy(
        local_charm,
        resources=resources,
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
async def test_upgrade(ops_test: OpsTest, righty_upgrade_charm: Path) -> None:
    mongodb_application = ops_test.model.applications[APP_NAME]
    await mongodb_application.refresh(path=righty_upgrade_charm)

    initial_version = Path("workload_version").read_text().strip()
    [major, minor, patch] = initial_version.split(".")
    new_version = f"{major}.{int(minor)+1}.{patch}+testupgrade"

    logger.info("Wait for refresh to stall")
    await ops_test.model.block_until(
        lambda: mongodb_application.status == "blocked", timeout=UPGRADE_TIMEOUT
    )
    assert (
        "resume-refresh" in mongodb_application.status_message
    ), "MongoDB application status not indicating that user should resume refresh."

    for attempt in tenacity.Retrying(
        reraise=True,
        stop=tenacity.stop_after_delay(SMALL_TIMEOUT),
        wait=tenacity.wait_fixed(10),
    ):
        with attempt:
            assert "+testupgrade" in get_juju_status(
                ops_test.model.name, APP_NAME
            ), "None of the units are upgraded"

    logger.info("Running resume-refresh on the leader unit")
    leader_id = await get_leader_id(ops_test, APP_NAME)
    leader_unit = ops_test.model.units.get(f"{APP_NAME}/{leader_id}")
    action = await leader_unit.run_action("resume-refresh")
    await action.wait()

    await ops_test.model.wait_for_idle(
        [APP_NAME],
        status="active",
        idle_period=30,
        timeout=UPGRADE_TIMEOUT,
    )

    for unit in mongodb_application.units:
        workload_version = await get_workload_version(ops_test, unit.name)
        assert workload_version == new_version
        assert initial_version != workload_version
