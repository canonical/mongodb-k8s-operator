#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import pytest
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest

from .helpers import (
    APP_NAME,
    DEPLOYMENT_TIMEOUT,
    METADATA,
    UNIT_IDS,
    check_or_scale_app,
    get_address_of_unit,
    get_app_name,
)

logger = logging.getLogger(__name__)


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    app_name = await get_app_name(ops_test)
    if app_name:
        return await check_or_scale_app(ops_test, app_name, len(UNIT_IDS))

    app_name = APP_NAME
    # build and deploy charm from local source folder
    charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=app_name,
        num_units=len(UNIT_IDS),
        series="noble",
        trust=True,
    )

    # issuing dummy update_status just to trigger an event
    await ops_test.model.set_config({"update-status-hook-interval": "10s"})

    # TODO: remove raise_on_error when we move to juju 3.5 (DPE-4996)
    await ops_test.model.wait_for_idle(
        apps=[app_name],
        status="active",
        raise_on_blocked=True,
        timeout=DEPLOYMENT_TIMEOUT,
        raise_on_error=False,
    )
    assert ops_test.model.applications[app_name].units[0].workload_status == "active"

    # effectively disable the update status from firing
    await ops_test.model.set_config({"update-status-hook-interval": "60m"})


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
@pytest.mark.parametrize("unit_id", UNIT_IDS)
async def test_application_is_up(ops_test: OpsTest, unit_id: int):
    address = await get_address_of_unit(ops_test, unit_id=unit_id)
    response = MongoClient(address, directConnection=True).admin.command("ping")
    assert response["ok"] == 1
