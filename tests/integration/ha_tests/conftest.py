# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from typing import Optional

import pytest


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
