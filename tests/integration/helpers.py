# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import math
from datetime import datetime
from pathlib import Path
from random import choices
from string import ascii_lowercase, digits
from types import SimpleNamespace
from typing import List, Optional

import yaml
from pytest_operator.plugin import OpsTest
from tenacity import retry, stop_after_attempt, wait_fixed

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
UNIT_IDS = [0, 1, 2]

TEST_DOCUMENTS = """[
    {
        \"uid\": 123,
        \"label\": \"Lorem\",
        \"price\": 2.3,
        \"currency\": \"eur\",
        \"exp_date\": \"2022-12-12\"
    },
    {
        \"uid\": 3456,
        \"label\": \"Ipsum\",
        \"price\": 18,
        \"currency\": \"usd\",
        \"exp_date\": \"2023-01-13\"
    }
]"""

SERIES = "jammy"

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


async def get_password(ops_test: OpsTest, unit_id: int, username="operator") -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    action = await ops_test.model.units.get(f"{APP_NAME}/{unit_id}").run_action(
        "get-password", **{"username": username}
    )
    action = await action.wait()
    return action.results["password"]


async def get_mongo_cmd(ops_test: OpsTest, unit_name: str):
    ls_code, _, _ = await ops_test.juju(f"ssh --container {unit_name} ls /usr/bin/mongosh")

    mongo_cmd = "/usr/bin/mongo" if ls_code != 0 else "/usr/bin/mongosh"
    return mongo_cmd


async def mongodb_uri(ops_test: OpsTest, unit_ids: List[int] = None) -> str:
    if unit_ids is None:
        unit_ids = UNIT_IDS

    addresses = [await get_address_of_unit(ops_test, unit_id) for unit_id in unit_ids]
    hosts = ",".join(addresses)
    password = await get_password(ops_test, unit_id=0)

    return f"mongodb://operator:{password}@{hosts}/admin"


# useful, as sometimes, the mongo request returns nothing on the first try
@retry(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True)
async def run_mongo_op(
    ops_test: OpsTest,
    mongo_op: str,
    mongo_uri: str = None,
    suffix: str = "",
    expecting_output: bool = True,
    stringify: bool = True,
    ignore_errors: bool = False,
) -> SimpleNamespace():
    """Runs provided MongoDB operation in a separate container."""
    if mongo_uri is None:
        mongo_uri = await mongodb_uri(ops_test)

    if stringify:
        mongo_cmd = f"mongo --quiet --eval 'JSON.stringify({mongo_op})' {mongo_uri}{suffix}"
    else:
        mongo_cmd = f"mongo --quiet --eval '{mongo_op}' {mongo_uri}{suffix}"

    logger.info("Running mongo command: %r", mongo_cmd)
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

    output = SimpleNamespace(failed=False, succeeded=False, data=None)

    ret_code, stdout, stderr = await ops_test.run(*kubectl_cmd)
    if ret_code != 0:
        logger.error("code %r; stdout %r; stderr: %r", ret_code, stdout, stderr)
        output.failed = True
        output.data = {
            "code": ret_code,
            "stdout": stdout,
            "stderr": stderr,
        }
        return output

    output.succeeded = True
    if expecting_output:
        try:
            output.data = json.loads(stdout)
        except Exception:
            logger.error(
                "Could not serialize the output into json.{}{}".format(
                    f"\n\tSTDOUT:\n\t {stdout}" if stdout else "",
                    f"\n\tSTDERR:\n\t {stderr}" if stderr else "",
                )
            )
            if not ignore_errors:
                raise
            else:
                output.data = stdout
    return output


def primary_host(rs_status_data: dict) -> Optional[str]:
    """Returns the primary host in the replica set or None if none was elected."""
    primary_list = [
        member["name"]
        for member in rs_status_data["members"]
        if member["stateStr"].upper() == "PRIMARY"
    ]

    if not primary_list:
        return None

    return primary_list[0]


async def check_if_test_documents_stored(
    ops_test: OpsTest, collection: str, mongo_uri: str = None
) -> None:
    # decide whether to pass a mongo_uri or replication set to the "run_mongo_op" function
    run_mongo_op_kwargs = {"suffix": f"?replicaSet={APP_NAME}"}
    if mongo_uri is not None:
        run_mongo_op_kwargs["mongo_uri"] = mongo_uri

    # serialize the str test documents into json
    o_test_docs = json.loads(TEST_DOCUMENTS)

    # query filter
    query_filter = json.dumps({"$or": [{"uid": test_doc["uid"]} for test_doc in o_test_docs]})

    count_documents = await run_mongo_op(
        ops_test, f"db.{collection}.countDocuments({query_filter})", **run_mongo_op_kwargs
    )
    assert count_documents.succeeded and count_documents.data == 2

    # descending order to match insertion order of the test documents
    find_documents = await run_mongo_op(
        ops_test,
        f"db.{collection}.find({query_filter}).sort({{uid: 1}}).toArray()",
        **run_mongo_op_kwargs,
    )
    assert find_documents.succeeded and len(find_documents.data) == 2

    for index, test_doc in zip(range(len(o_test_docs)), o_test_docs):
        db_doc = find_documents.data[index]

        for key, val in test_doc.items():
            assert db_doc[key] == val


async def secondary_mongo_uris_with_sync_delay(ops_test: OpsTest, rs_status_data):
    """Returns the list of secondaries and their sync delay with the master.

    Returns the ascending list of Secondaries, the first secondary is the
    one with the lowest data sync delay.
    """
    primary_optime_date = [
        datetime.strptime(member["optimeDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
        for member in rs_status_data["members"]
        if member["stateStr"].upper() == "PRIMARY"
    ][0]

    secondaries = []
    for member in rs_status_data["members"]:
        if member["stateStr"].upper() != "SECONDARY":
            continue

        unit_id = member["name"].split(".")[0].split("-")[-1]
        member_optime_date = datetime.strptime(member["optimeDate"], "%Y-%m-%dT%H:%M:%S.%fZ")

        host = await mongodb_uri(ops_test, [unit_id])
        delay_seconds = (primary_optime_date - member_optime_date).total_seconds()

        secondaries.append({"uri": host, "delay": math.fabs(delay_seconds)})

    secondaries.sort(key=lambda o: o["delay"])

    return secondaries


def generate_collection_id() -> str:
    new_id = "".join(choices(ascii_lowercase + digits, k=4)).replace("_", "")
    return f"collection_{new_id}"
