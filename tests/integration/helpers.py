# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dateutil.parser import parse
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_delay, wait_fixed

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
UNIT_IDS = [0, 1, 2]
DEPLOYMENT_TIMEOUT = 2000

SERIES = "jammy"


logger = logging.getLogger(__name__)


class Status:
    """Model class for status."""

    def __init__(self, value: str, since: str, message: Optional[str] = None):
        self.value = value
        self.since = parse(since, ignoretz=True)
        self.message = message


class Unit:
    """Model class for a Unit, with properties widely used."""

    def __init__(
        self,
        id: int,
        name: str,
        ip: str,
        hostname: str,
        is_leader: bool,
        workload_status: Status,
        agent_status: Status,
        app_status: Status,
    ):
        self.id = id
        self.name = name
        self.ip = ip
        self.hostname = hostname
        self.is_leader = is_leader
        self.workload_status = workload_status
        self.agent_status = agent_status
        self.app_status = app_status

    def dump(self) -> Dict[str, Any]:
        """To json."""
        result = {}
        for key, val in vars(self).items():
            result[key] = vars(val) if isinstance(val, Status) else val
        return result


async def get_app_name(ops_test: OpsTest, test_deployments: List[str] = []) -> str:
    """Returns the name of the cluster running MongoDB.

    This is important since not all deployments of the MongoDB charm have the application name
    "mongodb".

    Note: if multiple clusters are running MongoDB this will return the one first found.
    """
    status = await ops_test.model.get_status()
    for app in ops_test.model.applications:
        # note that format of the charm field is not exactly "mongodb" but instead takes the form
        # of `local:focal/mongodb-6`
        if "mongodb" in status["applications"][app]["charm"]:
            logger.debug("Found mongodb app named '%s'", app)

            if app in test_deployments:
                logger.debug("mongodb app named '%s', was deployed by the test, not by user", app)
                continue

            return app

    return None


async def check_or_scale_app(ops_test: OpsTest, user_app_name: str, required_units: int) -> None:
    """A helper function that scales existing cluster if necessary."""
    # check if we need to scale
    current_units = len(ops_test.model.applications[user_app_name].units)

    count = required_units - current_units
    if required_units == current_units:
        return
    count = required_units - current_units
    await ops_test.model.applications[user_app_name].scale(scale_change=count)
    # TODO : Remove raise_on_error when we move to juju 3.5 (DPE-4996)
    await ops_test.model.wait_for_idle(
        apps=[user_app_name], status="active", raise_on_error=False, timeout=2000
    )


async def get_unit_hostname(ops_test: OpsTest, unit_id: int, app: str) -> str:
    """Get the hostname of a specific unit."""
    _, hostname, _ = await ops_test.juju("ssh", f"{app}/{unit_id}", "hostname")
    return hostname.strip()


async def get_raw_application(ops_test: OpsTest, app: str) -> Dict[str, Any]:
    """Get raw application details."""
    ret_code, stdout, stderr = await ops_test.juju(
        *f"status --model {ops_test.model.info.name} {app} --format=json".split()
    )
    if ret_code != 0:
        logger.error(f"Invalid return [{ret_code=}]: {stderr=}")
        raise Exception(f"[{ret_code=}] {stderr=}")
    return json.loads(stdout)["applications"][app]


async def get_application_units(ops_test: OpsTest, app: str) -> List[Unit]:
    """Get fully detailed units of an application."""
    # Juju incorrectly reports the IP addresses after the network is restored this is reported as a
    # bug here: https://github.com/juju/python-libjuju/issues/738. Once this bug is resolved use of
    # `get_unit_ip` should be replaced with `.public_address`
    raw_app = await get_raw_application(ops_test, app)
    units = []
    for u_name, unit in raw_app["units"].items():
        unit_id = int(u_name.split("/")[-1])
        if not unit.get("address", False):
            # unit not ready yet...
            continue

        unit = Unit(
            id=unit_id,
            name=u_name.replace("/", "-"),
            ip=unit["address"],
            hostname=await get_unit_hostname(ops_test, unit_id, app),
            is_leader=unit.get("leader", False),
            workload_status=Status(
                value=unit["workload-status"]["current"],
                since=unit["workload-status"]["since"],
                message=unit["workload-status"].get("message"),
            ),
            agent_status=Status(
                value=unit["juju-status"]["current"],
                since=unit["juju-status"]["since"],
            ),
            app_status=Status(
                value=raw_app["application-status"]["current"],
                since=raw_app["application-status"]["since"],
                message=raw_app["application-status"].get("message"),
            ),
        )

        units.append(unit)

    return units


async def check_all_units_blocked_with_status(
    ops_test: OpsTest, db_app_name: str, status: Optional[str]
) -> None:
    # this is necessary because ops_model.units does not update the unit statuses
    for unit in await get_application_units(ops_test, db_app_name):
        assert (
            unit.workload_status.value == "blocked"
        ), f"unit {unit.name} not in blocked state, in {unit.workload_status.value}"
        if status:
            # We can have extra info but we care for the most important status
            assert (
                status in unit.workload_status.message
            ), f"unit {unit.name} not in blocked state, in {unit.workload_status.value}"


async def wait_for_mongodb_units_blocked(
    ops_test: OpsTest, db_app_name: str, status: Optional[str] = None, timeout=20
) -> None:
    """Waits for units of MongoDB to be in the blocked state.

    This is necessary because the MongoDB app can report a different status than the units.
    """
    hook_interval_key = "update-status-hook-interval"
    try:
        old_interval = (await ops_test.model.get_config())[hook_interval_key]
        await ops_test.model.set_config({hook_interval_key: "1m"})
        for attempt in Retrying(stop=stop_after_delay(timeout), wait=wait_fixed(1), reraise=True):
            with attempt:
                await check_all_units_blocked_with_status(ops_test, db_app_name, status)
    finally:
        await ops_test.model.set_config({hook_interval_key: old_interval})
