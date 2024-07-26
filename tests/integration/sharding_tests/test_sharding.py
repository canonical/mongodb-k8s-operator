#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from pytest_operator.plugin import OpsTest

from ..ha_tests.helpers import get_mongo_client
from ..helpers import METADATA, wait_for_mongodb_units_blocked
from .helpers import has_correct_shards

SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
SHARD_THREE_APP_NAME = "shard-three"
SHARD_APPS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, SHARD_THREE_APP_NAME]
CONFIG_SERVER_APP_NAME = "config-server-one"
CLUSTER_APPS = [
    CONFIG_SERVER_APP_NAME,
    SHARD_ONE_APP_NAME,
    SHARD_TWO_APP_NAME,
    SHARD_THREE_APP_NAME,
]
SHARD_REL_NAME = "sharding"
CONFIG_SERVER_REL_NAME = "config-server"
OPERATOR_USERNAME = "operator"
OPERATOR_PASSWORD = "operator-password"

CONFIG_SERVER_NEEDS_SHARD_STATUS = "missing relation to shard(s)"
SHARD_NEEDS_CONFIG_SERVER_STATUS = "missing relation to config server"


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy a sharded cluster."""
    my_charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}

    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        num_units=2,
        config={"role": "config-server"},
        application_name=CONFIG_SERVER_APP_NAME,
    )
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        num_units=2,
        config={"role": "shard"},
        application_name=SHARD_ONE_APP_NAME,
    )
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        num_units=2,
        config={"role": "shard"},
        application_name=SHARD_TWO_APP_NAME,
    )
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        num_units=2,
        config={"role": "shard"},
        application_name=SHARD_THREE_APP_NAME,
    )

    await ops_test.model.wait_for_idle(
        apps=[
            CONFIG_SERVER_APP_NAME,
            SHARD_ONE_APP_NAME,
            SHARD_TWO_APP_NAME,
            SHARD_THREE_APP_NAME,
        ],
        idle_period=20,
        raise_on_blocked=False,
    )

    # verify that Charmed MongoDB is blocked and reports incorrect credentials
    await wait_for_mongodb_units_blocked(
        ops_test, CONFIG_SERVER_APP_NAME, status=CONFIG_SERVER_NEEDS_SHARD_STATUS, timeout=300
    )
    await wait_for_mongodb_units_blocked(
        ops_test, SHARD_ONE_APP_NAME, status=SHARD_NEEDS_CONFIG_SERVER_STATUS, timeout=300
    )
    await wait_for_mongodb_units_blocked(
        ops_test, SHARD_TWO_APP_NAME, status=SHARD_NEEDS_CONFIG_SERVER_STATUS, timeout=300
    )
    await wait_for_mongodb_units_blocked(
        ops_test, SHARD_THREE_APP_NAME, status=SHARD_NEEDS_CONFIG_SERVER_STATUS, timeout=300
    )


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_cluster_active(ops_test: OpsTest) -> None:
    """Tests the integration of cluster components works without error."""
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )
    await ops_test.model.integrate(
        f"{SHARD_TWO_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )
    await ops_test.model.integrate(
        f"{SHARD_THREE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[
            CONFIG_SERVER_APP_NAME,
            SHARD_ONE_APP_NAME,
            SHARD_TWO_APP_NAME,
            SHARD_THREE_APP_NAME,
        ],
        idle_period=15,
        status="active",
    )

    mongos_client = await get_mongo_client(ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True)

    # verify sharded cluster config
    assert has_correct_shards(
        mongos_client,
        expected_shards=[SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, SHARD_THREE_APP_NAME],
    ), "Config server did not process config properly"
