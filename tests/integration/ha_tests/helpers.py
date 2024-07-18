# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import string
import subprocess
import tarfile
import tempfile
import time
from asyncio import gather
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import kubernetes as kubernetes
import ops
import yaml
from juju.unit import Unit
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest
from tenacity import (
    RetryError,
    Retrying,
    retry,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

from ..helpers import (
    APP_NAME,
    get_app_name,
    get_mongo_cmd,
    get_password,
    mongodb_uri,
    primary_host,
)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
MONGODB_CONTAINER_NAME = "mongod"
MONGODB_SERVICE_NAME = "mongod"
MONGOD_PROCESS_NAME = "mongod"
MONGODB_LOG_PATH = "/var/log/mongodb/mongodb.log"
APPLICATION_DEFAULT_APP_NAME = "application"
TIMEOUT = 15 * 60
TEST_DB = "continuous_writes_database"
TEST_COLLECTION = "test_collection"
ANOTHER_DATABASE_APP_NAME = "another-database"
EXCLUDED_APPS = [ANOTHER_DATABASE_APP_NAME]

logger = logging.getLogger(__name__)

mongodb_charm, application_charm = None, None


class ProcessRunningError(Exception):
    """Raised when a process is running when it is not expected to be."""


async def get_application_name(ops_test: OpsTest, application_name: str) -> str:
    """Returns the Application in the juju model that matches the provided application name.

    This enables us to retrieve the name of the deployed application in an existing model, while
     ignoring some test specific applications.
    Note: if multiple applications with the application name exist, the first one found will be
     returned.
    """
    status = await ops_test.model.get_status()

    for application in ops_test.model.applications:
        # note that format of the charm field is not exactly "mongodb" but instead takes the form
        # of `local:focal/mongodb-6`
        if (
            application_name in status["applications"][application]["charm"]
            and application not in EXCLUDED_APPS
        ):
            return application

    return None


async def scale_application(
    ops_test: OpsTest, application_name: str, desired_count: int, wait: bool = True
) -> None:
    """Scale a given application to the desired unit count.

    Args:
        ops_test: The ops test framework
        application_name: The name of the application
        desired_count: The number of units to scale to
        wait: Boolean indicating whether to wait until units
            reach desired count
    """
    if len(ops_test.model.applications[application_name].units) == desired_count:
        return
    await ops_test.model.applications[application_name].scale(desired_count)

    if desired_count > 0 and wait:
        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[application_name],
                status="active",
                timeout=TIMEOUT,
                wait_for_exact_units=desired_count,
                raise_on_blocked=True,
            )

    assert len(ops_test.model.applications[application_name].units) == desired_count


async def relate_mongodb_and_application(
    ops_test: OpsTest, mongodb_application_name: str, application_name: str
) -> None:
    """Relates the mongodb and application charms.

    Args:
        ops_test: The ops test framework
        mongodb_application_name: The mongodb charm application name
        application_name: The continuous writes test charm application name
    """
    if is_relation_joined(ops_test, "database", "database"):
        return

    await ops_test.model.relate(
        f"{application_name}:database", f"{mongodb_application_name}:database"
    )
    await ops_test.model.block_until(lambda: is_relation_joined(ops_test, "database", "database"))

    await ops_test.model.wait_for_idle(
        apps=[mongodb_application_name, application_name],
        status="active",
        raise_on_blocked=True,
        timeout=TIMEOUT,
    )


async def deploy_and_scale_mongodb(
    ops_test: OpsTest,
    check_for_existing_application: bool = True,
    mongodb_application_name: str = APP_NAME,
    num_units: int = 3,
    charm_path: Optional[Path] = None,
) -> str:
    """Deploys and scales the mongodb application charm.

    Args:
        ops_test: The ops test framework
        check_for_existing_application: Whether to check for existing mongodb applications
            in the model
        mongodb_application_name: The name of the mongodb application if it is to be deployed
        num_units: The desired number of units
        charm_path: The location of a prebuilt mongodb-k8s charm
    """
    application_name = await get_application_name(ops_test, mongodb_application_name)

    if check_for_existing_application and application_name:
        await scale_application(ops_test, application_name, num_units)

        return application_name

    global mongodb_charm
    # if provided an existing charm, use it instead of building
    if charm_path:
        mongodb_charm = charm_path
    if not mongodb_charm:
        charm = await ops_test.build_charm(".")
        # Cache the built charm to avoid rebuilding it between tests
        mongodb_charm = charm

    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}

    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            mongodb_charm,
            application_name=mongodb_application_name,
            resources=resources,
            num_units=num_units,
            series="jammy",
        )

        await ops_test.model.wait_for_idle(
            apps=[mongodb_application_name],
            status="active",
            raise_on_blocked=True,
            timeout=TIMEOUT,
        )

    return mongodb_application_name


async def deploy_and_scale_application(ops_test: OpsTest) -> str:
    """Deploys and scales the test application charm.

    Args:
        ops_test: The ops test framework
    """
    application_name = await get_application_name(ops_test, "application")

    if application_name:
        await scale_application(ops_test, application_name, 1)

        return application_name

    global application_charm
    if not application_charm:
        charm = await ops_test.build_charm("./tests/integration/ha_tests/application_charm")
        # Cache the built charm to avoid rebuilding it between tests
        application_charm = charm

    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_DEFAULT_APP_NAME,
            num_units=1,
            series="jammy",
        )

        await ops_test.model.wait_for_idle(
            apps=[APPLICATION_DEFAULT_APP_NAME],
            status="waiting",
            raise_on_blocked=True,
            timeout=TIMEOUT,
        )

    return APPLICATION_DEFAULT_APP_NAME


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


async def get_process_pid(
    ops_test: OpsTest, unit_name: str, container_name: str, process: str
) -> int:
    """Return the pid of a process running in a given unit.

    Args:
        ops_test: The ops test object passed into every test case
        unit_name: The name of the unit
        container_name: The name of the container in the unit
        process: The process name to search for
    Returns:
        A integer for the process id
    """
    get_pid_commands = [
        "ssh",
        "--container",
        container_name,
        unit_name,
        "pgrep",
        "-x",
        process,
    ]
    return_code, pid, _ = await ops_test.juju(*get_pid_commands)

    assert (
        return_code == 0
    ), f"Failed getting pid, unit={unit_name}, container={container_name}, process={process}"

    stripped_pid = pid.strip().replace("\r", "").replace("\n", "")
    assert (
        stripped_pid
    ), f"Failed stripping pid, unit={unit_name}, container={container_name}, process={process}, {pid}"

    return int(stripped_pid)


async def send_signal_to_pod_container_process(
    ops_test: OpsTest, unit_name: str, container_name: str, process: str, signal_code: str
) -> None:
    """Send the specified signal to a pod container process.

    Args:
        ops_test: The ops test framework
        unit_name: The name of the unit to send signal to
        container_name: The name of the container to send signal to
        process: The name of the process to send signal to
        signal_code: The code of the signal to send
    """
    cmd = [
        "ssh",
        "--container",
        container_name,
        unit_name,
        "pkill",
        f"-{signal_code}",
        process,
    ]
    ret_code, _, _ = await ops_test.juju(*cmd)

    assert (
        ret_code == 0
    ), f"Failed to send {signal_code} signal, unit={unit_name}, container={container_name}, process={process}"


def host_to_unit(host: str) -> Optional[str]:
    return "/".join(host.split(".")[0].rsplit("-", 1)) if host else None


async def mongod_ready(ops_test: OpsTest, unit: int) -> bool:
    """Verifies replica is running and available."""
    client = MongoClient(await mongodb_uri(ops_test, [unit]), directConnection=True)
    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 5), wait=wait_fixed(3)):
            with attempt:
                # The ping command is cheap and does not require auth.
                client.admin.command("ping")
    except RetryError:
        return False
    finally:
        client.close()

    return True


async def get_replica_set_primary(
    ops_test: OpsTest,
    excluded: List[str] = [],
    application_name=APP_NAME,
    use_subprocess_to_get_password=False,
) -> Optional[Unit]:
    """Returns the primary unit name based no the replica set host."""
    with await get_mongo_client(
        ops_test, excluded, use_subprocess_to_get_password=use_subprocess_to_get_password
    ) as client:
        data = client.admin.command("replSetGetStatus")
    unit_name = host_to_unit(primary_host(data))

    if unit_name:
        mongodb_name = await get_application_name(ops_test, application_name)
        for unit in ops_test.model.applications[mongodb_name].units:
            logger.info(
                f"Unit name: {unit.name}. Target unit name: {unit_name}, {unit.name == unit_name}"
            )
            if unit.name == unit_name:
                return unit
        logger.error(
            f"Target unit name {unit_name} not found in {ops_test.model.applications[mongodb_name].units}"
        )


async def count_primaries(ops_test: OpsTest) -> int:
    """Returns the number of primaries in a replica set."""
    with await get_mongo_client(ops_test) as client:
        data = client.admin.command("replSetGetStatus")

    return len([member for member in data["members"] if member["stateStr"] == "PRIMARY"])


async def fetch_replica_set_members(
    ops_test: OpsTest, use_subprocess_to_get_password=False
) -> List[str]:
    """Fetches the hosts listed as replica set members in the MongoDB replica set configuration.

    Args:
        ops_test: reference to deployment.
        use_subprocess_to_get_password: whether to use subprocess to get password.
    """
    # connect to replica set uri
    # get ips from MongoDB replica set configuration
    with await get_mongo_client(
        ops_test, use_subprocess_to_get_password=use_subprocess_to_get_password
    ) as client:
        data = client.admin.command("replSetGetConfig")

    return [member["host"].split(":")[0] for member in data["config"]["members"]]


async def get_direct_mongo_client(
    ops_test: OpsTest, unit: str, use_subprocess_to_get_password=False
) -> MongoClient:
    """Returns a direct mongodb client to specific unit."""
    url = await mongodb_uri(
        ops_test,
        [int(unit.split("/")[1])],
        use_subprocess_to_get_password=use_subprocess_to_get_password,
    )
    return MongoClient(url, directConnection=True)


async def get_mongo_client(
    ops_test: OpsTest, excluded: List[str] = [], use_subprocess_to_get_password=False
) -> MongoClient:
    """Returns a direct mongodb client potentially passing over some of the units."""
    mongodb_name = await get_application_name(ops_test, APP_NAME)
    for unit in ops_test.model.applications[mongodb_name].units:
        if unit.name not in excluded and unit.workload_status == "active":
            url = await mongodb_uri(
                ops_test,
                [int(unit.name.split("/")[1])],
                use_subprocess_to_get_password=use_subprocess_to_get_password,
            )
            return MongoClient(url, directConnection=True)
    assert False, "No fitting unit could be found"


async def find_unit(ops_test: OpsTest, leader: bool) -> ops.model.Unit:
    """Helper function identifies a unit, based on need for leader or non-leader."""
    ret_unit = None
    app = await get_application_name(ops_test, APP_NAME)
    for unit in ops_test.model.applications[app].units:
        if await unit.is_leader_from_status() == leader:
            ret_unit = unit

    return ret_unit


async def get_units_hostnames(ops_test: OpsTest) -> List[str]:
    """Generates k8s hostnames based on unit names."""
    return [
        f"{unit.name.replace('/', '-')}.mongodb-k8s-endpoints"
        for unit in ops_test.model.applications[APP_NAME].units
    ]


async def check_db_stepped_down(ops_test: OpsTest, sigterm_time: datetime):
    # loop through all units that aren't the old primary
    app_name = await get_app_name(ops_test)
    for unit in ops_test.model.applications[app_name].units:
        # these log files can get quite large. According to the Juju team the 'run' command
        # cannot be used for more than 16MB of data so it is best to use juju ssh or juju scp.
        logs = subprocess.check_output(
            f"JUJU_MODEL={ops_test.model_full_name} juju ssh  --container mongod {unit.name} 'cat {MONGODB_LOG_PATH}'",
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        )

        filtered_logs = filter(filter_logs_by_step_down, logs.split("\n"))

        for log in filtered_logs:
            item = json.loads(log)
            step_down_time = convert_time(item["t"]["$date"])
            if step_down_time >= sigterm_time:
                return

    assert False, "primary departed without stepping down."


async def set_log_level(ops_test: OpsTest, level: int, component: str = None) -> None:
    """Sets a given loglevel for a given component for each mongodb unit."""
    pass_unit = ops_test.model.applications[APP_NAME].units[0].name
    cmd = [
        "ssh",
        "--container",
        MONGODB_CONTAINER_NAME,
        "",
        await get_mongo_cmd(ops_test, pass_unit),
        "-u",
        "operator",
        "-p",
        await get_password(ops_test, int(pass_unit.split("/")[1])),
        "--quiet",
        "--eval",
        "",
    ]

    awaits = []
    for unit in ops_test.model.applications[APP_NAME].units:
        cmd[3] = unit.name
        cmd[-1] = cmd[-1] = f"\"db.setLogLevel({level}, '{component}')\""
        awaits.append(ops_test.juju(*cmd))
    await gather(*awaits)


async def get_total_writes(ops_test: OpsTest) -> int:
    """Gets the total writes from the test application action."""
    application_name = await get_application_name(ops_test, "application")
    application_unit = ops_test.model.applications[application_name].units[0]
    stop_writes_action = await application_unit.run_action("stop-continuous-writes")
    await stop_writes_action.wait()
    total_expected_writes = int(stop_writes_action.results["writes"])
    assert total_expected_writes > 0, "error while getting total writes."
    return total_expected_writes


async def kubectl_delete(ops_test: OpsTest, unit: ops.model.Unit, wait: bool = True) -> None:
    """Delete the underlying pod for a unit."""
    kubectl_cmd = (
        "microk8s",
        "kubectl",
        "delete",
        "pod",
        f"--wait={wait}",
        f"-n{ops_test.model_name}",
        unit.name.replace("/", "-"),
    )
    ret_code, _, _ = await ops_test.run(*kubectl_cmd)
    assert ret_code == 0, "Unit failed to delete"


async def insert_record_in_collection(ops_test: OpsTest) -> None:
    """Inserts the Focal Fossa data into the MongoDB cluster via primary replica."""
    primary = await get_replica_set_primary(ops_test)
    with await get_direct_mongo_client(ops_test, primary.name) as client:
        db = client["new-db"]
        test_collection = db["test_ubuntu_collection"]
        test_collection.insert_one({"release_name": "Focal Fossa", "version": 20.04, "LTS": True})


async def find_record_in_collection(ops_test: OpsTest) -> None:
    """Checks that all the nodes in the cluster have the Focal Fossa data."""
    app = await get_application_name(ops_test, APP_NAME)
    for unit in ops_test.model.applications[app].units:
        with await get_direct_mongo_client(ops_test, unit.name) as client:
            db = client["new-db"]
            test_collection = db["test_ubuntu_collection"]
            query = test_collection.find({}, {"release_name": 1})
            release_name = query[0]["release_name"]
        assert release_name == "Focal Fossa"


async def verify_writes(ops_test: OpsTest) -> int:
    """Verifies that no writes to the cluster were missed.

    Gets the total writes according to the test application and verifies against all nodes
    """
    app = await get_application_name(ops_test, APP_NAME)
    primary = await get_replica_set_primary(ops_test)

    total_expected_writes = await get_total_writes(ops_test)
    for unit in ops_test.model.applications[app].units:
        role = "Primary" if unit.name == primary.name else "Secondary"
        with await get_direct_mongo_client(ops_test, unit.name) as client:
            actual_writes = client[TEST_DB][TEST_COLLECTION].count_documents({})
        assert (
            total_expected_writes == actual_writes
        ), f"{role} {unit.name} missed writes to the db."
    return total_expected_writes


async def get_other_mongodb_direct_client(ops_test: OpsTest, app_name: str) -> MongoClient:
    """Returns a direct mongodb client to the second mongodb cluster.

    Since the second mongodb-k8s application will have separate IPs and credentials, connection URI
     must be generated separately.
    """
    unit = ops_test.model.applications[app_name].units[0]
    action = await unit.run_action("get-password")
    action = await action.wait()
    password = action.results["password"]
    status = await ops_test.model.get_status()
    address = status["applications"][app_name]["units"][unit.name]["address"]

    return MongoClient(f"mongodb://operator:{password}@{address}/admin", directConnection=True)


def retrieve_entries(client, db_name, collection_name, query_field):
    """Retries entries from a specified collection from a provided client."""
    db = client[db_name]
    test_collection = db[collection_name]

    # read all entries from original cluster
    cursor = test_collection.find({})
    cluster_entries = set()
    for document in cursor:
        cluster_entries.add(document[query_field])

    return cluster_entries


def deploy_chaos_mesh(namespace: str) -> None:
    """Deploy chaos mesh to the provided namespace.

    Chaos mesh can them be used by the tests to simulate a variety of failures.

    Args:
        namespace: The namespace to deploy chaos mesh to
    """
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")

    subprocess.check_output(
        " ".join(
            [
                "tests/integration/ha_tests/scripts/deploy_chaos_mesh.sh",
                namespace,
            ]
        ),
        shell=True,
        env=env,
    )


def destroy_chaos_mesh(namespace: str) -> None:
    """Destroy chaos mesh on a provided namespace.

    Cleans up the test K8S from test related dependencies.

    Args:
        namespace: The namespace to deploy chaos mesh to
    """
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")

    subprocess.check_output(
        f"tests/integration/ha_tests/scripts/destroy_chaos_mesh.sh {namespace}",
        shell=True,
        env=env,
    )


def isolate_instance_from_cluster(ops_test: OpsTest, unit_name: str) -> None:
    """Apply a NetworkChaos file to use chaos-mesh to simulate a network cut."""
    with tempfile.NamedTemporaryFile(dir=".") as temp_file:
        # Generates a manifest for chaosmesh to simulate network failure for a pod
        with open(
            "tests/integration/ha_tests/manifests/chaos_network_loss.yml", "r"
        ) as chaos_network_loss_file:
            template = string.Template(chaos_network_loss_file.read())
            chaos_network_loss = template.substitute(
                namespace=ops_test.model.info.name,
                pod=unit_name.replace("/", "-"),
            )

            temp_file.write(str.encode(chaos_network_loss))
            temp_file.flush()

        # Apply the generated manifest, chaosmesh would then make the pod inaccessible
        env = os.environ
        env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
        command_result = subprocess.check_output(
            " ".join(["microk8s", "kubectl", "apply", "-f", temp_file.name]),
            shell=True,
            env=env,
            stderr=subprocess.STDOUT,
        )
        logger.info("Result of isolating unit from cluster is '%s'", command_result)


def remove_instance_isolation(ops_test: OpsTest) -> None:
    """Delete the NetworkChaos that is isolating the primary unit of the cluster."""
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
    subprocess.check_output(
        f"microk8s kubectl -n {ops_test.model.info.name} delete networkchaos network-loss-primary",
        shell=True,
        env=env,
    )


@retry(
    stop=stop_after_attempt(10),
    wait=wait_fixed(5),
    reraise=True,
)
async def wait_until_unit_in_status(
    ops_test: OpsTest, unit_to_check: Unit, online_unit: Unit, status: str
) -> None:
    """Waits until a replica is in the provided status as reported by MongoDB or timeout occurs."""
    with await get_direct_mongo_client(
        ops_test, online_unit.name, use_subprocess_to_get_password=True
    ) as client:
        data = client.admin.command("replSetGetStatus")

    for member in data["members"]:
        if unit_to_check.name == host_to_unit(member["name"].split(":")[0]):
            assert (
                member["stateStr"] == status
            ), f"{unit_to_check.name} status is not {status}. Actual status: {member['stateStr']}"
            return
    assert False, f"{unit_to_check.name} not found"


async def retrieve_current_mongod_command(ops_test: OpsTest, unit_name) -> str:
    pid = await get_process_pid(ops_test, unit_name, MONGODB_CONTAINER_NAME, MONGOD_PROCESS_NAME)

    get_cmd_command = [
        "ssh",
        "--container",
        MONGODB_CONTAINER_NAME,
        unit_name,
        "cat",
        f"/proc/{pid}/cmdline",
    ]

    return_code, mongod_cmd, _ = await ops_test.juju(*get_cmd_command)
    assert (
        len(mongod_cmd) > 0
    ), f"Failed getting CMD, unit={unit_name}, container={MONGODB_CONTAINER_NAME}, pid={pid}, return_code={return_code}"
    assert (
        return_code == 0
    ), f"Failed getting CMD, unit={unit_name}, container={MONGODB_CONTAINER_NAME}, pid={pid}, return_code={return_code}"

    return mongod_cmd.replace("--", " --")


async def update_pebble_plans(ops_test: OpsTest, override: Dict[str, str]) -> None:
    """Injects a given override in mongod services and replans."""
    layer = json.dumps({"services": {MONGODB_SERVICE_NAME: {"override": "merge", **override}}})
    base_cmd = (
        "ssh",
        "--container",
        MONGODB_CONTAINER_NAME,
    )
    now = datetime.now().isoformat()

    for unit in ops_test.model.applications[APP_NAME].units:
        echo_cmd = (
            *base_cmd,
            unit.name,
            "echo",
            f"'{layer}'",
            ">",
            "/ha_test.yaml",
        )
        ret_code, _, _ = await ops_test.juju(*echo_cmd)
        assert ret_code == 0, f"Failed to create layer for {unit.name}"
        add_plan_cmd = (
            *base_cmd,
            unit.name,
            "/charm/bin/pebble",
            "add",
            # layer name label should be unique
            f"ha_test_{now}",
            "ha_test.yaml",
        )
        ret_code, _, _ = await ops_test.juju(*add_plan_cmd)
        assert ret_code == 0, f"Failed to set pebble plan for unit {unit.name}"

        replan_cmd = (
            *base_cmd,
            unit.name,
            "/charm/bin/pebble",
            "replan",
        )
        ret_code, _, _ = await ops_test.juju(*replan_cmd)
        assert ret_code == 0, f"Failed to replan for unit {unit.name}"


async def reused_storage(ops_test: OpsTest, reused_unit: Unit, removal_time: datetime) -> None:
    """Verifies storage is reused by the mongo daemon.

    MongoDB startup message indicates storage reuse:
        If member transitions to STARTUP2 from STARTUP then it is syncing/getting data from
        primary.
        If member transitions to STARTUP2 from REMOVED then it is re-using the storage we
        provided.
    """
    cat_cmd = [
        "ssh",
        "--container",
        MONGODB_CONTAINER_NAME,
        reused_unit.name,
        f"cat {MONGODB_LOG_PATH}",
    ]

    return_code, logs, _ = await ops_test.juju(*cat_cmd)

    assert (
        return_code == 0
    ), f"Failed catting mongodb logs, unit={reused_unit.name}, container={MONGODB_CONTAINER_NAME}"

    filtered_logs = filter(filter_logs_by_startup, logs.split("\n"))

    for log in filtered_logs:
        item = json.loads(log)
        reuse_time = convert_time(item["t"]["$date"])
        if reuse_time > removal_time:
            return True

    return False


def filter_logs_by_step_down(log):
    return True if "Starting an election due to step up request" in log else False


def filter_logs_by_startup(log):
    return True if '"newState":"STARTUP2","oldState":"REMOVED"' in log else False


def convert_time(time_as_str: str) -> int:
    """Converts a string time representation to an integer time representation."""
    # Remove the timezone information (the +00:00 part) for simplicity
    time_as_str = time_as_str[:-6]

    # parse time representation, provided in this format: 'YYYY-MM-DDTHH:MM:SS.MMM'
    d = datetime.strptime(time_as_str, "%Y-%m-%dT%H:%M:%S.%f")

    return time.mktime(d.timetuple()) + d.microsecond / 1_000_000


def get_highest_unit(ops_test: OpsTest, app_name: str) -> Unit:
    """Retrieves the most recently added unit to the MongoDB application."""
    num_units = len(ops_test.model.applications[app_name].units)
    highest_unit_name = f"mongodb-k8s/{num_units-1}"
    for unit in ops_test.model.applications[app_name].units:
        if unit.name == highest_unit_name:
            return unit


async def are_all_db_processes_down(ops_test: OpsTest, process: str) -> bool:
    """Verifies that all units of the charm do not have the DB process running."""
    app = await get_application_name(ops_test, APP_NAME)

    # '/' can effect the results of `pgrep`, to search for processes with '/' it is
    # necessary to match the full name, i.e. '-f'
    if "/" in process:
        pgrep_cmd = ("pgrep", "-f", process)
    else:
        pgrep_cmd = ("pgrep", "-x", process)

    try:
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                for unit in ops_test.model.applications[app].units:
                    _, raw_pid, _ = await ops_test.juju(
                        "ssh", "--container", MONGODB_CONTAINER_NAME, unit.name, *pgrep_cmd
                    )

                    # If something was returned, there is a running process.
                    if len(raw_pid) > 0:
                        raise ProcessRunningError
    except RetryError:
        return False

    return True


def modify_pebble_restart_delay(
    ops_test: OpsTest,
    unit_name: str,
    pebble_plan_path: str,
    ensure_replan: bool = False,
) -> None:
    """Modify the pebble restart delay of the underlying process.

    Args:
        ops_test: The ops test framework
        unit_name: The name of unit to extend the pebble restart delay for
        pebble_plan_path: Path to the file with the modified pebble plan
        ensure_replan: Whether to check that the replan command succeeded
    """
    kubernetes.config.load_kube_config()
    client = kubernetes.client.api.core_v1_api.CoreV1Api()

    pod_name = unit_name.replace("/", "-")
    container_name = "mongod"
    service_name = "mongod"
    now = datetime.now().isoformat()

    copy_file_into_pod(
        client,
        ops_test.model.info.name,
        pod_name,
        container_name,
        f"/tmp/pebble_plan_{now}.yml",
        pebble_plan_path,
    )

    add_to_pebble_layer_commands = (
        f"/charm/bin/pebble add --combine {service_name} /tmp/pebble_plan_{now}.yml"
    )
    response = kubernetes.stream.stream(
        client.connect_get_namespaced_pod_exec,
        pod_name,
        ops_test.model.info.name,
        container=container_name,
        command=add_to_pebble_layer_commands.split(),
        stdin=False,
        stdout=True,
        stderr=True,
        tty=False,
        _preload_content=False,
    )
    response.run_forever(timeout=5)
    assert (
        response.returncode == 0
    ), f"Failed to add to pebble layer, unit={unit_name}, container={container_name}, service={service_name}"

    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            replan_pebble_layer_commands = "/charm/bin/pebble replan"
            response = kubernetes.stream.stream(
                client.connect_get_namespaced_pod_exec,
                pod_name,
                ops_test.model.info.name,
                container=container_name,
                command=replan_pebble_layer_commands.split(),
                stdin=False,
                stdout=True,
                stderr=True,
                tty=False,
                _preload_content=False,
            )
            response.run_forever(timeout=60)
            if ensure_replan:
                assert (
                    response.returncode == 0
                ), f"Failed to replan pebble layer, unit={unit_name}, container={container_name}, service={service_name}"


def copy_file_into_pod(
    client: kubernetes.client.api.core_v1_api.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_name: str,
    source_path: str,
    destination_path: str,
) -> None:
    """Copy file contents into pod.

    Args:
        client: The kubernetes CoreV1Api client
        namespace: The namespace of the pod to copy files to
        pod_name: The name of the pod to copy files to
        container_name: The name of the pod container to copy files to
        source_path: The path to which the file should be copied over
        destination_path: The path of the file which needs to be copied over
    """
    try:
        exec_command = ["tar", "xvf", "-", "-C", "/"]

        api_response = kubernetes.stream.stream(
            client.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            container=container_name,
            command=exec_command,
            stdin=True,
            stdout=True,
            stderr=True,
            tty=False,
            _preload_content=False,
        )

        with tempfile.TemporaryFile() as tar_buffer:
            with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
                tar.add(destination_path, source_path)

            tar_buffer.seek(0)
            commands = []
            commands.append(tar_buffer.read())

            while api_response.is_open():
                api_response.update(timeout=1)

                if commands:
                    command = commands.pop(0)
                    api_response.write_stdin(command.decode())
                else:
                    break

            api_response.close()
    except kubernetes.client.rest.ApiException:
        assert False
