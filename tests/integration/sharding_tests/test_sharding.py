#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from pytest_operator.plugin import OpsTest

from ..ha_tests.helpers import get_mongo_client
from ..helpers import METADATA, wait_for_mongodb_units_blocked
from .helpers import (
    has_correct_shards,
    shard_has_databases,
    verify_data_mongodb,
    write_data_to_mongodb,
)

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


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_sharding(ops_test: OpsTest) -> None:
    """Tests writing data to mongos gets propagated to shards."""
    # write data to mongos on both shards.
    mongos_client = await get_mongo_client(ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True)

    # write data to shard two
    write_data_to_mongodb(
        mongos_client,
        db_name="animals_database_1",
        coll_name="horses",
        content={"horse-breed": "unicorn", "real": True},
    )
    mongos_client.admin.command("movePrimary", "animals_database_1", to=SHARD_TWO_APP_NAME)

    # write data to shard three
    write_data_to_mongodb(
        mongos_client,
        db_name="animals_database_2",
        coll_name="horses",
        content={"horse-breed": "pegasus", "real": True},
    )
    mongos_client.admin.command("movePrimary", "animals_database_2", to=SHARD_THREE_APP_NAME)

    # log into shard two verify data
    shard_two_client = await get_mongo_client(ops_test, app_name=SHARD_TWO_APP_NAME, mongos=False)

    has_correct_data = verify_data_mongodb(
        shard_two_client,
        db_name="animals_database_1",
        coll_name="horses",
        key="horse-breed",
        value="unicorn",
    )
    assert has_correct_data, "data not written to shard-two"

    # log into shard 2 verify data
    shard_three_client = await get_mongo_client(
        ops_test, app_name=SHARD_THREE_APP_NAME, mongos=False
    )

    has_correct_data = verify_data_mongodb(
        shard_three_client,
        db_name="animals_database_2",
        coll_name="horses",
        key="horse-breed",
        value="pegasus",
    )
    assert has_correct_data, "data not written to shard-three"


@pytest.mark.group(1)
async def test_shard_removal(ops_test: OpsTest) -> None:
    """Test shard removal.

    This test also verifies that:
    - Databases that are using this shard as a primary are moved.
    - The balancer is turned back on if turned off.
    - Config server supp    orts removing multiple shards.
    """
    # turn off balancer.
    mongos_client = await get_mongo_client(ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True)
    mongos_client.admin.command("balancerStop")
    balancer_state = mongos_client.admin.command("balancerStatus")
    assert balancer_state["mode"] == "off", "balancer was not successfully turned off"

    # remove two shards at the same time
    await ops_test.model.applications[CONFIG_SERVER_APP_NAME].remove_relation(
        f"{SHARD_TWO_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )
    await ops_test.model.applications[CONFIG_SERVER_APP_NAME].remove_relation(
        f"{SHARD_THREE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[
            CONFIG_SERVER_APP_NAME,
            SHARD_THREE_APP_NAME,
            SHARD_THREE_APP_NAME,
        ],
        idle_period=15,
        status="active",
    )

    # verify that config server turned back on the balancer
    balancer_state = mongos_client.admin.command("balancerStatus")
    assert balancer_state["mode"] != "off", "balancer not turned back on from config server"

    # verify sharded cluster config
    assert has_correct_shards(
        mongos_client, expected_shards=[SHARD_ONE_APP_NAME]
    ), "Config server did not process config properly"

    # verify no data lost
    assert shard_has_databases(
        mongos_client,
        shard_name=SHARD_ONE_APP_NAME,
        expected_databases_on_shard=["animals_database_1", "animals_database_2"],
    ), "Not all databases on final shard"


@pytest.mark.group(1)
async def test_removal_of_non_primary_shard(ops_test: OpsTest):
    """Tests safe removal of a shard that is not primary."""
    # add back a shard so we can safely remove a shard.
    await ops_test.model.integrate(
        f"{SHARD_TWO_APP_NAME}:{SHARD_REL_NAME}",
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
        raise_on_error=False,
    )

    await ops_test.model.applications[CONFIG_SERVER_APP_NAME].remove_relation(
        f"{SHARD_TWO_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME],
        idle_period=15,
        status="active",
        raise_on_error=False,
    )

    mongos_client = await get_mongo_client(ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True)

    # verify sharded cluster config
    assert has_correct_shards(
        mongos_client, expected_shards=[SHARD_ONE_APP_NAME]
    ), "Config server did not process config properly"

    # verify no data lost
    assert shard_has_databases(
        mongos_client,
        shard_name=SHARD_ONE_APP_NAME,
        expected_databases_on_shard=["animals_database_1", "animals_database_2"],
    ), "Not all databases on final shard"


@pytest.mark.group(1)
async def test_unconventual_shard_removal(ops_test: OpsTest):
    """Tests that removing a shard application safely drains data.

    It is preferred that users remove-relations instead of removing shard applications. But we do
    support removing shard applications in a safe way.
    """
    # add back a shard so we can safely remove a shard.
    await ops_test.model.integrate(
        f"{SHARD_TWO_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[SHARD_TWO_APP_NAME],
        idle_period=15,
        status="active",
        raise_on_error=False,
    )

    await ops_test.model.applications[SHARD_TWO_APP_NAME].scale(scale_change=-1)
    await ops_test.model.wait_for_idle(
        apps=[SHARD_TWO_APP_NAME],
        idle_period=15,
        status="active",
        raise_on_error=False,
    )

    await ops_test.model.remove_application(SHARD_TWO_APP_NAME, block_until_done=True)

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME],
        idle_period=15,
        status="active",
        raise_on_error=False,
    )

    mongos_client = await get_mongo_client(ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True)

    # verify sharded cluster config
    assert has_correct_shards(
        mongos_client, expected_shards=[SHARD_ONE_APP_NAME]
    ), "Config server did not process config properly"

    # verify no data lost
    assert shard_has_databases(
        mongos_client,
        shard_name=SHARD_ONE_APP_NAME,
        expected_databases_on_shard=["animals_database_1", "animals_database_2"],
    ), "Not all databases on final shard"
