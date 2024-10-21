#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

from pytest_operator.plugin import OpsTest

from ..backup_tests import helpers as backup_helpers

logger = logging.getLogger(__name__)


async def assert_successful_run_upgrade_sequence(
    ops_test: OpsTest, app_name: str, new_charm: Path
) -> None:
    """Runs the upgrade sequence on a given app."""
    leader_unit = await backup_helpers.get_leader_unit(ops_test, app_name)
    action = await leader_unit.run_action("pre-refresh-check")
    await action.wait()
    assert action.status == "completed", "pre-refresh-check failed, expected to succeed."

    logger.info(f"Upgrading {app_name}")

    await ops_test.model.applications[app_name].refresh(path=new_charm)
    await ops_test.model.wait_for_idle(apps=[app_name], timeout=1000, idle_period=30)

    # resume upgrade only needs to be ran when:
    # 1. there are more than one units in the application
    # 2. AND the underlying workload was updated
    if len(ops_test.model.applications[app_name].units) < 2:
        return

    if "resume-refresh" not in ops_test.model.applications[app_name].status_message:
        return

    logger.info(f"Calling resume-refresh for {app_name}")
    action = await leader_unit.run_action("resume-refresh")
    await action.wait()
    assert (
        action.status == "completed"
    ), f"resume-refresh failed, expected to succeed. ({action.results})"

    await ops_test.model.wait_for_idle(apps=[app_name], timeout=1000, idle_period=30)


async def get_workload_version(ops_test: OpsTest, unit_name: str) -> str:
    """Get the workload version of the deployed router charm."""
    return_code, output, _ = await ops_test.juju(
        "ssh",
        unit_name,
        "sudo",
        "cat",
        f"/var/lib/juju/agents/unit-{unit_name.replace('/', '-')}/charm/workload_version",
    )

    assert return_code == 0
    return output.strip()
