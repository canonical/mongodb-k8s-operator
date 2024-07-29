# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import math
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from random import choices
from string import ascii_lowercase, digits
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import yaml
from dateutil.parser import parse
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, retry, stop_after_attempt, stop_after_delay, wait_fixed

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
UNIT_IDS = [0, 1, 2]
MONGOS_PORT = 27018
MONGOD_PORT = 27017

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

HELPER_MONGO_VERSION = "6.0.11"
HELPER_MONGO_POD_NAME = "mongodb-helper"


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


async def get_leader_id(ops_test: OpsTest, app_name: str = APP_NAME) -> int:
    """Returns the unit number of the juju leader unit."""
    for unit in ops_test.model.applications[app_name].units:
        if await unit.is_leader_from_status():
            return int(unit.entity_id.split("/")[-1])

    assert (
        False
    ), f"Failed to find unit leader for {app_name} using 'unit.is_leader_from_status()' !!!"


async def get_address_of_unit(ops_test: OpsTest, unit_id: int, app_name: str = APP_NAME) -> str:
    """Retrieves the address of the unit based on provided id."""
    status = await ops_test.model.get_status()
    return status["applications"][app_name]["units"][f"{app_name}/{unit_id}"]["address"]


async def get_password(
    ops_test: OpsTest, unit_id: int, username="operator", app_name: str = APP_NAME
) -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    action = await ops_test.model.units.get(f"{app_name}/{unit_id}").run_action(
        "get-password", **{"username": username}
    )
    action = await action.wait()
    return action.results["password"]


async def set_password(
    ops_test: OpsTest,
    unit_id: int,
    username: str = "operator",
    password: str = "secret",
    app_name: str = APP_NAME,
) -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    action = await ops_test.model.units.get(f"{app_name}/{unit_id}").run_action(
        "set-password", **{"username": username, "password": password}
    )
    action = await action.wait()
    return action.results


async def get_mongo_cmd(ops_test: OpsTest, unit_name: str):
    complete_command = f"ssh --container mongod {unit_name} ls /usr/bin/mongosh"
    ls_code, _, stderr = await ops_test.juju(*complete_command.split())
    match ls_code:
        case 0:
            return "/usr/bin/mongosh"
        case _:
            logger.info(f"mongosh not found. Reason: '{stderr}'. Using /usr/bin/mongo")
            return "/usr/bin/mongo"


async def mongodb_uri(
    ops_test: OpsTest,
    unit_ids: List[int] | None = None,
    use_subprocess_to_get_password=False,
    port=MONGOD_PORT,
    app_name: str = APP_NAME,
) -> str:
    if unit_ids is None:
        unit_ids = UNIT_IDS

    addresses = [await get_address_of_unit(ops_test, unit_id, app_name) for unit_id in unit_ids]
    hosts = [f"{host}:{port}" for host in addresses]
    hosts = ",".join(hosts)
    if use_subprocess_to_get_password:
        password = get_password_using_subprocess(ops_test, app_name)
    else:
        password = await get_password(ops_test, 0, app_name=app_name)
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
    expect_json_load: bool = True,
) -> SimpleNamespace():
    """Runs provided MongoDB operation in a separate container."""
    if mongo_uri is None:
        mongo_uri = await mongodb_uri(ops_test)

    if stringify:
        mongo_cmd = f"mongosh --quiet --eval 'EJSON.stringify({mongo_op})' {mongo_uri}{suffix}"
    else:
        mongo_cmd = f"mongosh --quiet --eval '{mongo_op}' {mongo_uri}{suffix}"

    logger.info("Running mongo command: %r", mongo_cmd)

    create_pod_if_not_exists(
        ops_test.model_name, HELPER_MONGO_POD_NAME, "mongo", f"mongo:{HELPER_MONGO_VERSION}"
    )

    while not is_pod_ready(ops_test.model_name, HELPER_MONGO_POD_NAME):
        logger.info("Waiting for pod to be ready...")
        time.sleep(5)

    kubectl_cmd = (
        "microk8s",
        "kubectl",
        "exec",
        "-i",
        "-n",
        ops_test.model_name,
        HELPER_MONGO_POD_NAME,
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
        output.data = _process_mongo_operation_result(stdout, stderr, expect_json_load)
    logger.info("Done: '%s'", output)
    return output


def _process_mongo_operation_result(stdout, stderr, expect_json_load):
    try:
        return json.loads(stdout)
    except Exception:
        logger.error(
            "Could not serialize the output into json.{}{}".format(
                f"\n\tSTDOUT:\n\t {stdout}" if stdout else "",
                f"\n\tSTDERR:\n\t {stderr}" if stderr else "",
            )
        )
        logger.error(f"Failed to load operation result: {stdout} to json")
        if expect_json_load:
            raise
        else:
            try:
                logger.info("Attempt to cast to python dict manually")
                # cast to python dict
                dict_string = re.sub(r"(\w+)(\s*:\s*)", r'"\1"\2', stdout)
                dict_string = (
                    dict_string.replace("true", "True")
                    .replace("false", "False")
                    .replace("null", "None")
                )
                return eval(dict_string)
            except Exception:
                logger.error(f"Failed to cast response to python dict. Returning stdout: {stdout}")
                return stdout


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


async def get_application_relation_data(
    ops_test: OpsTest,
    application_name: str,
    relation_name: str,
    key: str,
    relation_id: str = None,
    relation_alias: str = None,
) -> Optional[str]:
    """Get relation data for an application.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        relation_name: name of the relation to get connection data from
        key: key of data to be retrieved
        relation_id: id of the relation to get connection data from
        relation_alias: alias of the relation (like a connection name)
            to get connection data from
    Returns:
        the that that was requested or None
            if no data in the relation
    Raises:
        ValueError if it's not possible to get application unit data
            or if there is no data for the particular relation endpoint
            and/or alias.
    """
    unit_name = f"{application_name}/0"
    raw_data = (await ops_test.juju("show-unit", unit_name))[1]

    if not raw_data:
        raise ValueError(f"no unit info could be grabbed for {unit_name}")
    data = yaml.safe_load(raw_data)

    # Filter the data based on the relation name.
    relation_data = [v for v in data[unit_name]["relation-info"] if v["endpoint"] == relation_name]

    if relation_id:
        # Filter the data based on the relation id.
        relation_data = [v for v in relation_data if v["relation-id"] == relation_id]

    if relation_alias:
        # Filter the data based on the cluster/relation alias.
        relation_data = [
            v
            for v in relation_data
            if json.loads(v["application-data"]["data"])["alias"] == relation_alias
        ]

    if len(relation_data) == 0:
        raise ValueError(
            f"no relation data could be grabbed on relation with endpoint {relation_name} and alias {relation_alias}"
        )

    return relation_data[0]["application-data"].get(key)


async def get_secret_id(ops_test, app_or_unit: Optional[str] = None) -> str:
    """Retrieve secret ID for an app or unit."""
    complete_command = "list-secrets"

    if app_or_unit:
        prefix = "unit" if app_or_unit[-1].isdigit() else "application"
        formated_app_or_unit = f"{prefix}-{app_or_unit}"
        if prefix == "unit":
            formated_app_or_unit = formated_app_or_unit.replace("/", "-")
        complete_command += f" --owner {formated_app_or_unit}"

    _, stdout, _ = await ops_test.juju(*complete_command.split())
    output_lines_split = [line.split() for line in stdout.strip().split("\n")]
    if app_or_unit:
        return [line[0] for line in output_lines_split if app_or_unit in line][0]

    return output_lines_split[1][0]


async def get_secret_content(ops_test, secret_id) -> Dict[str, str]:
    """Retrieve contents of a Juju Secret."""
    secret_id = secret_id.split("/")[-1]
    complete_command = f"show-secret {secret_id} --reveal --format=json"
    _, stdout, _ = await ops_test.juju(*complete_command.split())
    data = json.loads(stdout)
    return data[secret_id]["content"]["Data"]


def create_pod_if_not_exists(namespace, pod_name, container_name, image_name):
    """Create a pod if not already exists."""
    logger.info("Checking or creating helper mongo pod ...")
    get_pod_cmd = f"kubectl get pod {pod_name} -n {namespace} -o json"
    result = subprocess.run(get_pod_cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        logger.info(f"pod '{pod_name}' in namespace '{namespace}' already exists.")
        return

    if "NotFound" in result.stderr:
        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": namespace},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": container_name,
                        "image": image_name,
                        "command": ["/bin/bash"],
                        "stdin": True,
                        "tty": True,
                    }
                ],
            },
        }

        pod_manifest_json = json.dumps(pod_manifest)

        create_pod_cmd = f"echo '{pod_manifest_json}' | kubectl apply -f -"
        create_result = subprocess.run(create_pod_cmd, shell=True, capture_output=True, text=True)

        if create_result.returncode == 0:
            logger.info(f"pod '{pod_name}' created in namespace '{namespace}'.")
        else:
            logger.error(f"Failed to create pod: {create_result.stderr}")
    else:
        logger.error(f"Failed to check pod existence: {result.stderr}")


def is_pod_ready(namespace, pod_name):
    """Checks that the pod is ready."""
    get_pod_cmd = f"kubectl get pod {pod_name} -n {namespace} -o json"
    result = subprocess.run(get_pod_cmd, shell=True, capture_output=True, text=True)
    logger.info(f"Checking pod {pod_name} is ready...")
    if result.returncode != 0:
        return False

    pod_info = json.loads(result.stdout)
    for condition in pod_info["status"].get("conditions", []):
        if condition["type"] == "Ready" and condition["status"] == "True":
            return True
    return False


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(30),
    reraise=True,
)
def get_password_using_subprocess(
    ops_test: OpsTest, username="operator", app_name=APP_NAME
) -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    cmd = ["juju", "switch", ops_test.model_name]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        logger.error(
            "Failed to get password. Can't switch to juju model: '%s'. Error '%s'",
            ops_test.model_name,
            result.stderr,
        )
        raise Exception(f"Failed to get password: {result.stderr}")
    cmd = ["juju", "run", f"{app_name}/leader", "get-password", f"username={username}"]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        logger.error("get-password command returned non 0 exit code: %s", result.stderr)
        raise Exception(f"get-password command returned non 0 exit code: {result.stderr}")
    try:
        password = result.stdout.decode("utf-8").split("password:")[-1].strip()
    except Exception as e:
        logger.error("Failed to get password: %s", e)
        raise Exception(f"Failed to get password: {e}")
    return password


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
    await ops_test.model.wait_for_idle(apps=[user_app_name], status="active", timeout=2000)


def audit_log_line_sanity_check(entry) -> bool:
    fields = ["atype", "ts", "local", "remote", "users", "roles", "param", "result"]
    for field in fields:
        if entry.get(field) is None:
            logger.error("Field '%s' not found in audit log entry \"%s\"", field, entry)
            return False
    return True


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
        ), f"unit {unit.name} not in blocked state, in {unit.workload_status}"
        if status:
            assert (
                unit.workload_status.message == status
            ), f"unit {unit.name} not in blocked state, in {unit.workload_status}"


async def wait_for_mongodb_units_blocked(
    ops_test: OpsTest, db_app_name: str, status: Optional[str] = None, timeout=20
) -> None:
    """Waits for units of MongoDB to be in the blocked state.

    This is necessary because the MongoDB app can report a different status than the units.
    """
    for attempt in Retrying(stop=stop_after_delay(timeout), wait=wait_fixed(1), reraise=True):
        with attempt:
            await check_all_units_blocked_with_status(ops_test, db_app_name, status)


def is_relation_joined(ops_test: OpsTest, endpoint_one: str, endpoint_two: str) -> bool:
    """Check if a relation is joined.

    Args:
        ops_test: The ops test object passed into every test case
        endpoint_one: The first endpoint of the relation
        endpoint_two: The second endpoint of the relation
    """
    for rel in ops_test.model.relations:
        endpoints = [endpoint.name for endpoint in rel.endpoints]
        if endpoint_one in endpoints and endpoint_two in endpoints:
            return True
    return False
