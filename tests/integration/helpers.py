# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from typing import Optional

import yaml
from pytest_operator.plugin import OpsTest

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
UNIT_IDS = [0, 1, 2]
logger = logging.getLogger(__name__)


async def get_leader_id(ops_test: OpsTest) -> int:
    """Returns the unit number of the juju leader unit."""
    leader_unit_id = 0
    for unit in ops_test.model.applications[APP_NAME].units:
        if await unit.is_leader_from_status():
            return leader_unit_id

        leader_unit_id += 1

    return leader_unit_id


async def get_address_of_unit(ops_test: OpsTest, unit_id: int) -> str:
    """Retrieves the address of the unit based on provided id."""
    status = await ops_test.model.get_status()
    return status["applications"][APP_NAME]["units"][f"{APP_NAME}/{unit_id}"]["address"]


async def get_password(ops_test: OpsTest, unit_id: int) -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    action = await ops_test.model.units.get(f"{APP_NAME}/{unit_id}").run_action(
        "get-admin-password"
    )
    action = await action.wait()
    return action.results["admin-password"]


async def run_mongo_op(ops_test: OpsTest, mongo_op: str):
    """Runs provided MongoDB operation in a separate container."""
    addresses = [await get_address_of_unit(ops_test, unit_id) for unit_id in UNIT_IDS]
    hosts = ",".join(addresses)
    password = await get_password(ops_test, unit_id=0)
    mongo_cmd = (
        f"mongo --quiet --eval 'JSON.stringify({mongo_op})' "
        f"mongodb://operator:{password}@{hosts}/admin"
    )
    kubectl_cmd = (
        "microk8s",
        "kubectl",
        "run",
        "--rm",
        "-i",
        "-q",
        "--restart=Never",
        "--command",
        f"--namespace={ops_test.model_name}",
        "mongo-test",
        "--image=mongo:4.4",
        "--",
        "sh",
        "-c",
        mongo_cmd,
    )

    ret_code, stdout, stderr = await ops_test.run(*kubectl_cmd)
    if ret_code != 0:
        logger.error("code %r; stdout %r; stderr: %r", ret_code, stdout, stderr)
        return None

    return json.loads(stdout)


def primary_host(rs_status) -> Optional[str]:
    """Returns the primary host in the replica set or None if none was elected"""
    primary_list = [member["name"]
                    for member in rs_status["members"]
                    if member["stateStr"].upper() == "PRIMARY"]

    if not primary_list:
        return None

    return primary_list[0]
