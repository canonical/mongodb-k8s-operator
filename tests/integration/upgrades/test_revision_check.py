#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from pytest_operator.plugin import OpsTest

from ..helpers import METADATA, wait_for_mongodb_units_blocked

MONGODB_K8S_CHARM = "mongodb-k8s"
SHARD_REL_NAME = "sharding"
CONFIG_SERVER_REL_NAME = "config-server"

LOCAL_SHARD_APP_NAME = "local-shard"
REMOTE_SHARD_APP_NAME = "remote-shard"
LOCAL_CONFIG_SERVER_APP_NAME = "local-config-server"
REMOTE_CONFIG_SERVER_APP_NAME = "remote-config-server"

CLUSTER_COMPONENTS = [
    LOCAL_SHARD_APP_NAME,
    REMOTE_SHARD_APP_NAME,
    LOCAL_CONFIG_SERVER_APP_NAME,
    REMOTE_CONFIG_SERVER_APP_NAME,
]


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    my_charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}

    await ops_test.model.deploy(
        MONGODB_K8S_CHARM,
        application_name=REMOTE_SHARD_APP_NAME,
        config={"role": "shard"},
        channel="edge",
    )

    await ops_test.model.deploy(
        MONGODB_K8S_CHARM,
        application_name=REMOTE_CONFIG_SERVER_APP_NAME,
        config={"role": "config-server"},
        channel="edge",
    )
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        config={"role": "config-server"},
        application_name=LOCAL_CONFIG_SERVER_APP_NAME,
    )
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        config={"role": "shard"},
        application_name=LOCAL_SHARD_APP_NAME,
    )

    await ops_test.model.wait_for_idle(apps=CLUSTER_COMPONENTS, idle_period=20)


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_local_config_server_reports_remote_shard(ops_test: OpsTest) -> None:
    """Tests that the local config server reports remote shard."""
    await ops_test.model.integrate(
        f"{REMOTE_SHARD_APP_NAME}:{SHARD_REL_NAME}",
        f"{LOCAL_CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[LOCAL_CONFIG_SERVER_APP_NAME],
        status="waiting",
        raise_on_blocked=False,
        idle_period=20,
    )

    config_server_unit = ops_test.model.applications[LOCAL_CONFIG_SERVER_APP_NAME].units[0]

    assert (
        "Waiting for shards to upgrade/downgrade to revision"
        in config_server_unit.workload_status_message
    ), "Config server does not correctly report mismatch in revision"


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_local_shard_reports_remote_config_server(ops_test: OpsTest) -> None:
    """Tests that the local shard reports remote config-server."""
    await ops_test.model.integrate(
        f"{LOCAL_SHARD_APP_NAME}:{SHARD_REL_NAME}",
        f"{REMOTE_CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await wait_for_mongodb_units_blocked(
        ops_test,
        LOCAL_SHARD_APP_NAME,
        timeout=300,
    )

    shard_unit = ops_test.model.applications[LOCAL_SHARD_APP_NAME].units[0]
    assert (
        "is not up-to date with config-server." in shard_unit.workload_status_message
    ), "Shard does not correctly report mismatch in revision"
