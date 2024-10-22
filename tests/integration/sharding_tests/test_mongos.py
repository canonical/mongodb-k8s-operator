#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


import pytest
import tenacity
from pymongo.errors import OperationFailure
from pytest_operator.plugin import OpsTest

from ..ha_tests.helpers import get_direct_mongo_client
from ..helpers import RESOURCES, is_relation_joined
from .helpers import count_users, get_related_username_password

SHARD_ONE_APP_NAME = "shard-one"
MONGOS_APP_NAME = "mongos-k8s"
CONFIG_SERVER_APP_NAME = "config-server-one"
SHARD_REL_NAME = "sharding"
CLUSTER_REL_NAME = "cluster"
CONFIG_SERVER_REL_NAME = "config-server"
NUMBER_OF_MONGOS_USERS_WHEN_NO_ROUTERS = 3  # operator-user, backup-user, and montior-user
TIMEOUT = 10 * 60
TWO_MINUTE_TIMEOUT = 2 * 60


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy a sharded cluster."""
    mongodb_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(
        mongodb_charm,
        resources=RESOURCES,
        num_units=1,
        config={"role": "config-server"},
        application_name=CONFIG_SERVER_APP_NAME,
        trust=True,
    )
    await ops_test.model.deploy(
        mongodb_charm,
        resources=RESOURCES,
        num_units=1,
        config={"role": "shard"},
        application_name=SHARD_ONE_APP_NAME,
        trust=True,
    )

    await ops_test.model.deploy(
        MONGOS_APP_NAME,
        channel="6/edge",
    )

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,  # cluster components are blocked waiting for integration.
        timeout=TIMEOUT,
        raise_on_error=False,  # Remove this once DPE-4996 is resolved
    )


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_connect_to_cluster_creates_user(ops_test: OpsTest) -> None:
    """Verifies that when the cluster is formed a new user is created."""
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[MONGOS_APP_NAME, SHARD_ONE_APP_NAME, CONFIG_SERVER_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,
        timeout=TIMEOUT,
        raise_on_error=False,
    )

    mongos_client = await get_direct_mongo_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )
    num_users = count_users(mongos_client)

    await ops_test.model.integrate(
        f"{MONGOS_APP_NAME}",
        f"{CONFIG_SERVER_APP_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, MONGOS_APP_NAME],
        status="active",
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_error=False,
    )

    await ops_test.model.block_until(
        lambda: is_relation_joined(
            ops_test,
            CLUSTER_REL_NAME,
            CLUSTER_REL_NAME,
        )
        is True,
        timeout=600,
    )

    num_users_after_integration = count_users(mongos_client)

    assert (
        num_users_after_integration == num_users + 1
    ), "Cluster did not create new users after integration."

    (username, password) = await get_related_username_password(
        ops_test, app_name=MONGOS_APP_NAME, relation_name=CLUSTER_REL_NAME
    )
    mongos_user_client = await get_direct_mongo_client(
        ops_test,
        app_name=CONFIG_SERVER_APP_NAME,
        mongos=True,
        username=username,
        password=password,
    )

    mongos_user_client.admin.command("dbStats")


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_disconnect_from_cluster_removes_user(ops_test: OpsTest) -> None:
    """Verifies that when the cluster is formed the client users are removed.

    Since mongos-k8s router supports multiple users, we expect that the removal of this
    integration will result in the removal of the mongos-k8s admin users and all of its clients.
    """
    # generate URI for new mongos user
    (username, password) = await get_related_username_password(
        ops_test, app_name=MONGOS_APP_NAME, relation_name=CLUSTER_REL_NAME
    )

    # generate URI for operator mongos user (i.e. admin)
    mongos_client = await get_direct_mongo_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )
    await ops_test.model.applications[MONGOS_APP_NAME].remove_relation(
        f"{MONGOS_APP_NAME}:cluster",
        f"{CONFIG_SERVER_APP_NAME}:cluster",
    )
    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, MONGOS_APP_NAME],
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_error=False,
    )
    num_users_after_removal = count_users(mongos_client)

    for attempt in tenacity.Retrying(
        reraise=True,
        stop=tenacity.stop_after_delay(TWO_MINUTE_TIMEOUT),
        wait=tenacity.wait_fixed(10),
    ):
        with attempt:
            assert (
                NUMBER_OF_MONGOS_USERS_WHEN_NO_ROUTERS == num_users_after_removal
            ), "Cluster did not remove user after integration removal."

    mongos_user_client = await get_direct_mongo_client(
        ops_test,
        app_name=CONFIG_SERVER_APP_NAME,
        mongos=True,
        username=username,
        password=password,
    )

    with pytest.raises(OperationFailure) as pymongo_error:
        mongos_user_client.admin.command("dbStats")

    assert pymongo_error.value.code == 18, "User still exists after relation was removed."
