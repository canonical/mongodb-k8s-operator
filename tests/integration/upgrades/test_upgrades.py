#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
from pytest_operator.plugin import OpsTest

from ..backup_tests import helpers as backup_helpers
from ..ha_tests import helpers as ha_helpers
from ..ha_tests.helpers import (
    count_writes,
    deploy_and_scale_application,
    find_unit,
    isolate_instance_from_cluster,
    relate_mongodb_and_application,
    remove_instance_isolation,
    wait_until_unit_in_status,
)
from ..helpers import check_or_scale_app, get_app_name, get_password
from .helpers import assert_successful_run_upgrade_sequence

logger = logging.getLogger(__name__)

WRITE_APP = "application"
MONGODB_CHARM_NAME = "mongodb-k8s"


@pytest.fixture(scope="module")
def chaos_mesh(ops_test: OpsTest) -> None:
    ha_helpers.deploy_chaos_mesh(ops_test.model.info.name)

    yield

    ha_helpers.destroy_chaos_mesh(ops_test.model.info.name)


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):

    await deploy_and_scale_application(ops_test)

    db_app_name = await get_app_name(ops_test)

    if db_app_name:
        await check_or_scale_app(ops_test, db_app_name, required_units=3)
        return
    else:
        await ops_test.model.deploy(MONGODB_CHARM_NAME, channel="6/edge", num_units=3, trust=True)

    db_app_name = await get_app_name(ops_test)
    await ops_test.model.wait_for_idle(
        apps=[db_app_name], status="active", timeout=1000, idle_period=120
    )

    await relate_mongodb_and_application(ops_test, db_app_name, WRITE_APP)


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_successful_upgrade(ops_test: OpsTest, continuous_writes) -> None:
    new_charm: Path = await ops_test.build_charm(".")
    db_app_name = await get_app_name(ops_test)
    await assert_successful_run_upgrade_sequence(ops_test, db_app_name, new_charm=new_charm)

    # verify that the no writes were skipped
    application_unit = ops_test.model.applications[WRITE_APP].units[0]
    stop_writes_action = await application_unit.run_action("stop-continuous-writes")
    await stop_writes_action.wait()
    total_expected_writes = int(stop_writes_action.results["writes"])
    assert total_expected_writes > 0, "error while getting expected writes."

    actual_writes = await count_writes(ops_test, app_name=db_app_name)
    assert total_expected_writes == actual_writes, "missed writes during upgrade procedure."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_preflight_check(ops_test: OpsTest) -> None:
    db_app_name = await get_app_name(ops_test)
    leader_unit = await backup_helpers.get_leader_unit(ops_test, db_app_name)

    logger.info("Calling pre-refresh-check")
    action = await leader_unit.run_action("pre-refresh-check")
    await action.wait()
    assert action.status == "completed", "pre-refresh-check failed, expected to succeed."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_preflight_check_failure(ops_test: OpsTest, chaos_mesh) -> None:
    db_app_name = await get_app_name(ops_test)
    leader_unit = await backup_helpers.get_leader_unit(ops_test, db_app_name)

    non_leader_unit = None
    for unit in ops_test.model.applications[db_app_name].units:
        if unit.name != leader_unit.name:
            non_leader_unit = unit
            break

    isolate_instance_from_cluster(ops_test, non_leader_unit.name)
    await wait_until_unit_in_status(
        ops_test, non_leader_unit, leader_unit, "(not reachable/healthy)"
    )

    logger.info("Calling pre-refresh-check")
    action = await leader_unit.run_action("pre-refresh-check")
    await action.wait()
    assert action.status == "failed", "pre-refresh-check succeeded, expected to fail."

    # restore network after test
    remove_instance_isolation(ops_test)
    await ops_test.model.wait_for_idle(
        apps=[db_app_name], status="active", timeout=1000, idle_period=30, raise_on_error=False
    )


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_upgrade_password_change_fail(ops_test: OpsTest):
    app_name = await get_app_name(ops_test)
    leader = await find_unit(ops_test, leader=True, app_name="mongodb-k8s")
    leader_id = leader.name.split("/")[1]
    current_password = await get_password(ops_test, leader_id, app_name="mongodb-k8s")
    new_charm = await ops_test.build_charm(".")

    await ops_test.model.applications[app_name].refresh(path=new_charm)

    leader = await find_unit(ops_test, leader=True, app_name="mongodb-k8s")
    leader_id = leader.name.split("/")[1]

    action = await ops_test.model.units.get(f"{app_name}/{leader_id}").run_action(
        "set-password", **{"username": "username", "password": "new-password"}
    )
    action = await action.wait()

    assert "Cannot set passwords while an upgrade is in progress." == action.message
    after_action_password = await get_password(ops_test, leader_id, app_name=app_name)
    assert current_password == after_action_password
