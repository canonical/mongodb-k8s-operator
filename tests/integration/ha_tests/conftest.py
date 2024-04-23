# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import shutil
from pathlib import Path
from typing import Optional

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import get_application_name


@pytest.fixture(scope="module")
async def local_application_charm(ops_test: OpsTest):
    """Build the application charm."""
    import os
    os.system("pwd; ls -la")

    application_name = await get_application_name(ops_test, "application")
    if application_name:
        return None

    shutil.copyfile(
        "./lib/charms/data_platform_libs/v0/data_interfaces.py",
        "./tests/integration/ha_tests/application-charm/lib/charms/data_platform_libs/v0/data_interfaces.py",
    )
    test_charm_path = "./tests/integration/ha_tests/application-charm"
    return await ops_test.build_charm(test_charm_path)


def pytest_addoption(parser):
    parser.addoption(
        "--mongodb_charm",
        action="store",
        default=None,
        help="The location of prebuilt mongodb-k8s charm",
    )


@pytest.fixture
def cmd_mongodb_charm(request) -> Optional[Path]:
    """Fixture to optionally pass a prebuilt charm to deploy."""
    charm_path = request.config.getoption("--mongodb_charm")
    if charm_path:
        path = Path(charm_path).absolute()
        if path.exists():
            return path
