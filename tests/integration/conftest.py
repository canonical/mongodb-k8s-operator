#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import os
import pathlib
import subprocess
import typing


def pytest_configure(config) -> None:
    if os.environ.get("CI") == "true":
        # Running in GitHub Actions; skip build step
        plugin = config.pluginmanager.get_plugin("pytest-operator")
        plugin.OpsTest.build_charm = _build_charm

        # Remove charmcraft dependency from `ops_test` fixture
        check_deps = plugin.check_deps
        plugin.check_deps = lambda *deps: check_deps(*(dep for dep in deps if dep != "charmcraft"))


async def _build_charm(self, charm_path: typing.Union[str, os.PathLike]) -> pathlib.Path:
    charm_path = pathlib.Path(charm_path)
    architecture = subprocess.run(
        ["dpkg", "--print-architecture"],
        capture_output=True,
        check=True,
        encoding="utf-8",
    ).stdout.strip()
    assert architecture in ("amd64", "arm64")
    packed_charms = list(charm_path.glob(f"*{architecture}.charm"))
    if len(packed_charms) == 1:
        # python-libjuju's model.deploy(), juju deploy, and juju bundle files expect local charms
        # to begin with `./` or `/` to distinguish them from Charmhub charms.
        # Therefore, we need to return an absolute path—a relative `pathlib.Path` does not start
        # with `./` when cast to a str.
        # (python-libjuju model.deploy() expects a str but will cast any input to a str as a
        # workaround for pytest-operator's non-compliant `build_charm` return type of
        # `pathlib.Path`.)
        return packed_charms[0].resolve(strict=True)
    elif len(packed_charms) > 1:
        raise ValueError(
            f"More than one matching .charm file found at {charm_path=} for {architecture=} and "
            f"Ubuntu 22.04: {packed_charms}."
        )
    else:
        raise ValueError(
            f"Unable to find .charm file for {architecture=} and Ubuntu 24.04 at {charm_path=}"
        )
