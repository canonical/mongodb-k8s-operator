#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import pytest
from pytest_operator.plugin import OpsTest

from ..backup_tests import helpers as backup_helpers
from ..ha_tests.helpers import (
    count_writes,
    deploy_and_scale_application,
    isolate_instance_from_cluster,
    relate_mongodb_and_application,
    remove_instance_isolation,
    wait_until_unit_in_status,
)
from ..ha_tests.test_ha import chaos_mesh, continuous_writes
from ..helpers import check_or_scale_app, get_app_name
from .helpers import assert_successful_run_upgrade_sequence

logger = logging.getLogger(__name__)

WRITE_APP = "application"
MONGODB_CHARM_NAME = "mongodb-k8s"


@pytest.mark.skip("skip until upgrades work has been released to charmhub")
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):

    await deploy_and_scale_application(ops_test)

    db_app_name = await get_app_name(ops_test)

    if db_app_name:
        await check_or_scale_app(ops_test, db_app_name, required_units=2)
        return
    else:
        await ops_test.model.deploy(MONGODB_CHARM_NAME, channel="6/edge", num_units=2)

    db_app_name = await get_app_name(ops_test)
    await ops_test.model.wait_for_idle(
        apps=[db_app_name], status="active", timeout=1000, idle_period=120
    )

    await relate_mongodb_and_application(ops_test, db_app_name, WRITE_APP)


@pytest.mark.skip("skip until upgrades work has been released to charmhub")
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_successful_upgrade(ops_test: OpsTest, continuous_writes) -> None:
    new_charm = await ops_test.build_charm(".")
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


@pytest.mark.skip("skip until upgrades work has been released to charmhub")
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_preflight_check(ops_test: OpsTest) -> None:
    db_app_name = await get_app_name(ops_test)
    leader_unit = await backup_helpers.get_leader_unit(ops_test, db_app_name)

    logger.info("Calling pre-upgrade-check")
    action = await leader_unit.run_action("pre-upgrade-check")
    await action.wait()
    assert action.status == "completed", "pre-upgrade-check failed, expected to succeed."


@pytest.mark.skip("skip until upgrades work has been released to charmhub")
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_preflight_check_failure(ops_test: OpsTest, chaos_mesh) -> None:
    db_app_name = await get_app_name(ops_test)
    leader_unit = await backup_helpers.get_leader_unit(ops_test, db_app_name)

    non_leader_unit = None
    for unit in ops_test.model.applications[db_app_name].units:
        if unit != leader_unit:
            non_leader_unit = unit
            break

    isolate_instance_from_cluster(ops_test, non_leader_unit.name)
    await wait_until_unit_in_status(
        ops_test, non_leader_unit, leader_unit, "(not reachable/healthy)"
    )

    logger.info("Calling pre-upgrade-check")
    action = await leader_unit.run_action("pre-upgrade-check")
    await action.wait()
    assert action.status == "completed", "pre-upgrade-check failed, expected to succeed."

    # restore network after test
    remove_instance_isolation(ops_test)
    await ops_test.model.wait_for_idle(
        apps=[db_app_name], status="active", timeout=1000, idle_period=30
    )
