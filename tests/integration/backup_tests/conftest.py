# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import shutil

import pytest
from pytest_operator.plugin import OpsTest

from ..ha_tests.helpers import get_application_name


@pytest.fixture(scope="module")
async def local_application_charm(ops_test: OpsTest):
    """Build the application charm."""
    application_name = await get_application_name(ops_test, "application")
    if application_name:
        return None

    shutil.copyfile(
        "./lib/charms/data_platform_libs/v0/data_interfaces.py",
        "./tests/integration/ha_tests/application-charm/lib/charms/data_platform_libs/v0/data_interfaces.py",
    )
    test_charm_path = "./tests/integration/ha_tests/application-charm"
    return await ops_test.build_charm(test_charm_path)
