#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from pytest_operator.plugin import OpsTest

from ..helpers import METADATA, wait_for_mongodb_units_blocked

S3_APP_NAME = "s3-integrator"
SHARD_ONE_APP_NAME = "shard"
CONFIG_SERVER_ONE_APP_NAME = "config-server-one"
CONFIG_SERVER_TWO_APP_NAME = "config-server-two"
REPLICATION_APP_NAME = "replication"
APP_CHARM_NAME = "application"
LEGACY_APP_CHARM_NAME = "legacy-application"
MONGOS_APP_NAME = "mongos"
MONGOS_HOST_APP_NAME = "application-host"

SHARDING_COMPONENTS = [SHARD_ONE_APP_NAME, CONFIG_SERVER_ONE_APP_NAME]

CONFIG_SERVER_REL_NAME = "config-server"
SHARD_REL_NAME = "sharding"
DATABASE_REL_NAME = "first-database"
LEGACY_RELATION_NAME = "obsolete"


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy a sharded cluster."""
    database_charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}

    await ops_test.model.deploy(
        database_charm,
        config={"role": "config-server"},
        resources=resources,
        application_name=CONFIG_SERVER_ONE_APP_NAME,
    )
    await ops_test.model.deploy(
        database_charm,
        config={"role": "config-server"},
        resources=resources,
        application_name=CONFIG_SERVER_TWO_APP_NAME,
    )
    await ops_test.model.deploy(
        database_charm,
        resources=resources,
        config={"role": "shard"},
        application_name=SHARD_ONE_APP_NAME,
    )

    await ops_test.model.deploy(S3_APP_NAME, channel="edge")

    await ops_test.model.wait_for_idle(
        apps=[
            CONFIG_SERVER_ONE_APP_NAME,
            CONFIG_SERVER_TWO_APP_NAME,
            SHARD_ONE_APP_NAME,
        ],
        idle_period=20,
        raise_on_blocked=False,
    )


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_shard_s3_relation(ops_test: OpsTest) -> None:
    """Verifies integrating a shard to s3-integrator fails."""
    # attempt to add a replication deployment as a shard to the config server.
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}",
        f"{S3_APP_NAME}",
    )

    await wait_for_mongodb_units_blocked(
        ops_test,
        SHARD_ONE_APP_NAME,
        status="Relation to s3-integrator is not supported, config role must be config-server",
        timeout=300,
    )

    # clean up relations
    await ops_test.model.applications[SHARD_ONE_APP_NAME].remove_relation(
        f"{S3_APP_NAME}:s3-credentials",
        f"{SHARD_ONE_APP_NAME}:s3-credentials",
    )
