#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
import pytest_asyncio
from pytest_operator.plugin import OpsTest

from .ha_tests.helpers import (
    deploy_chaos_mesh,
    destroy_chaos_mesh,
    get_application_name,
)

TEST_DB = "continuous_writes_database"
TEST_COLL = "continuous_writes_collection"


@pytest_asyncio.fixture
async def continuous_writes(ops_test: OpsTest) -> None:
    """Starts continuous writes to the MongoDB cluster and clear the writes at the end."""
    application_name = await get_application_name(ops_test, "application")

    application_unit = ops_test.model.applications[application_name].units[0]

    clear_writes_action = await application_unit.run_action(
        "clear-continuous-writes", **{"db-name": TEST_DB, "coll-name": TEST_COLL}
    )
    await clear_writes_action.wait()

    start_writes_action = await application_unit.run_action(
        "start-continuous-writes", **{"db-name": TEST_DB, "coll-name": TEST_COLL}
    )
    await start_writes_action.wait()

    yield

    clear_writes_action = await application_unit.run_action(
        "clear-continuous-writes", **{"db-name": TEST_DB, "coll-name": TEST_COLL}
    )
    await clear_writes_action.wait()


@pytest.fixture(scope="module")
def chaos_mesh(ops_test: OpsTest) -> None:
    deploy_chaos_mesh(ops_test.model.info.name)

    yield

    destroy_chaos_mesh(ops_test.model.info.name)
