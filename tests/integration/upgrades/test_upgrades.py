#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
from pytest_operator.plugin import OpsTest

from ..ha_tests.helpers import find_unit
from ..helpers import (
    APP_NAME,
    METADATA,
    check_or_scale_app,
    get_app_name,
    get_password,
    set_password,
)


@pytest.mark.skip("Missing upgrade code for now")
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    app_name = await get_app_name(ops_test)

    if app_name:
        await check_or_scale_app(ops_test, app_name, required_units=3)
        return

    app_name = APP_NAME
    new_charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}

    await ops_test.model.deploy(
        new_charm,
        resources=resources,
        application_name=app_name,
        num_units=3,
        series="jammy",
    )
    await ops_test.model.wait_for_idle(
        apps=[app_name], status="active", timeout=1000, idle_period=120
    )


@pytest.mark.skip("Missing upgrade code for now")
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_upgrade_password_change_fail(ops_test: OpsTest):
    app_name = await get_app_name(ops_test)
    leader_id = await find_unit(ops_test, leader=True, app_name=app_name)

    current_password = await get_password(ops_test, leader_id, app_name=app_name)
    new_charm = await ops_test.build_charm(".")
    await ops_test.model.applications[app_name].refresh(path=new_charm)
    results = await set_password(ops_test, leader_id, password="0xdeadbeef", app_name=app_name)

    assert results == "Cannot set passwords while an upgrade is in progress."

    after_action_password = await get_password(ops_test, leader_id, app_name=app_name)

    assert current_password == after_action_password
