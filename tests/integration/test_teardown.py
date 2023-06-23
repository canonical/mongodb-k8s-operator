#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import pytest
from pytest_operator.plugin import OpsTest

from .ha_tests.helpers import get_replica_set_primary as replica_set_primary
from .helpers import METADATA, SERIES

DATABASE_APP_NAME = "mongodb-k8s"
MEDIAN_REELECTION_TIME = 12

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    """Build the charm-under-test and deploy it to the model.

    Assert that the application is active and the replica set is healthy.
    """
    # build and deploy charm from local source folder
    charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=DATABASE_APP_NAME,
        num_units=1,
        series=SERIES,
    )

    # issuing dummy update_status just to trigger an event
    await ops_test.model.set_config({"update-status-hook-interval": "10s"})

    await ops_test.model.wait_for_idle(
        apps=[DATABASE_APP_NAME],
        status="active",
        raise_on_blocked=True,
        timeout=1000,
    )
    assert ops_test.model.applications[DATABASE_APP_NAME].units[0].workload_status == "active"

    # effectively disable the update status from firing
    await ops_test.model.set_config({"update-status-hook-interval": "60m"})


@pytest.mark.abort_on_fail
async def test_long_scale_up_scale_down_units(ops_test: OpsTest):
    """Scale up and down the application and verify the replica set is healthy."""
    scales = [2, -1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 7, -7]
    for count in scales:
        await scale_and_verify(ops_test, count=count)


async def scale_and_verify(ops_test: OpsTest, count: int):
    if count == 0:
        logger.warning("Skipping scale up/down by 0")
        return
    elif count > 0:
        logger.info(f"Scaling up by {count} units")
    else:
        logger.info(f"Scaling down by {abs(count)} units")

    await ops_test.model.applications[DATABASE_APP_NAME].scale(scale_change=count)

    await ops_test.model.wait_for_idle(
        apps=[DATABASE_APP_NAME],
        status="active",
        timeout=1000,
    )
    primary = await replica_set_primary(ops_test, application_name=DATABASE_APP_NAME)
    assert primary is not None, "Replica set has no primary"
