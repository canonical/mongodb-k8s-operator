# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.


def pytest_addoption(parser):
    parser.addoption(
        "--mongodb_charm",
        action="store",
        default=None,
        help="The location of prebuilt mongodb-k8s charm",
    )
