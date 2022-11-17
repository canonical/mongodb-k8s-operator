# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import subprocess
from asyncio import gather
from datetime import datetime
from pathlib import Path
from typing import List

import yaml
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

from tests.integration.helpers import (
    APP_NAME,
    get_password,
    mongodb_uri,
    primary_host,
    run_mongo_op,
)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
MONGODB_CONTAINER_NAME = "mongod"
APPLICATION_DEFAULT_APP_NAME = "application"
TIMEOUT = 15 * 60

mongodb_charm, application_charm = None, None


async def get_application_name(ops_test: OpsTest, application_name: str) -> str:
    """Returns the Application in the juju model that matches the provided application name.

    This enables us to retrieve the name of the deployed application in an existing model.

    Note: if multiple applications with the application name exist, the first one found will be
     returned.
    """
    status = await ops_test.model.get_status()

    for application in ops_test.model.applications:
        # note that format of the charm field is not exactly "mongodb" but instead takes the form
        # of `local:focal/mongodb-6`
        if application_name in status["applications"][application]["charm"]:
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
) -> str:
    """Deploys and scales the mongodb application charm.

    Args:
        ops_test: The ops test framework
        check_for_existing_application: Whether to check for existing mongodb applications
            in the model
        mongodb_application_name: The name of the mongodb application if it is to be deployed
    """
    application_name = await get_application_name(ops_test, "mongodb")

    if check_for_existing_application and application_name:
        await scale_application(ops_test, application_name, 3)

        return application_name

    global mongodb_charm
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
            num_units=3,
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
        charm = await ops_test.build_charm("./tests/integration/ha_tests/application_charm/")
        # Cache the built charm to avoid rebuilding it between tests
        application_charm = charm

    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_DEFAULT_APP_NAME,
            num_units=1,
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
        process,
    ]
    return_code, pid, _ = await ops_test.juju(*get_pid_commands)

    assert (
        return_code == 0
    ), f"Failed getting pid, unit={unit_name}, container={container_name}, process={process}"

    stripped_pid = pid.strip()
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


def host_to_unit(host: str) -> str:
    return "/".join(host.split(".")[0].rsplit("-", 1))


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


async def get_replica_set_primary(ops_test: OpsTest) -> str:
    """Returns the primary unit name based no the replica set host."""
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    return host_to_unit(primary_host(rs_status.data))


async def count_primaries(ops_test: OpsTest) -> int:
    """Returns the number of primaries in a replica set."""
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    primaries = 0
    # loop through all members in the replica set
    for member in rs_status.data["members"]:
        # check replica's current state
        if member["stateStr"] == "PRIMARY":
            primaries += 1

    return primaries


async def fetch_replica_set_members(ops_test: OpsTest) -> List[str]:
    """Fetches the IPs listed as replica set members in the MongoDB replica set configuration.

    Args:
        ops_test: reference to deployment.
    """
    # connect to replica set uri
    # get ips from MongoDB replica set configuration
    rs_config = await run_mongo_op(ops_test, "rs.config()")
    member_ips = []
    for member in rs_config.data["members"]:
        # get member ip without ":PORT"
        member_ips.append(member["host"].split(":")[0])

    return member_ips


async def get_mongo_client(
    ops_test: OpsTest, exact: str = None, excluded: List[str] = []
) -> MongoClient:
    """Returns a direct mongodb client to specific unit or passing over some of the units."""
    if exact:
        return MongoClient(
            await mongodb_uri(ops_test, [int(exact.split("/")[1])]), directConnection=True
        )
    mongodb_name = await get_application_name(ops_test, APP_NAME)
    for unit in ops_test.model.applications[mongodb_name].units:
        if unit.name not in excluded:
            return MongoClient(
                await mongodb_uri(ops_test, [int(unit.name.split("/")[1])]), directConnection=True
            )


async def get_units_hostnames(ops_test: OpsTest) -> List[str]:
    """Generates k8s hostnames based on unit names."""
    return [
        f"{unit.name.replace('/', '-')}.mongodb-k8s-endpoints"
        for unit in ops_test.model.applications[APP_NAME].units
    ]


async def db_step_down(ops_test: OpsTest, sigterm_time: datetime) -> None:
    """Pipes the k8s logs, looking for stepdown message."""
    kubectl_cmd = [
        "microk8s",
        "kubectl",
        "logs",
        f"-n{ops_test.model_name}",
        f"--since-time={sigterm_time.isoformat()}",
        "",
        "-c",
        MONGODB_CONTAINER_NAME,
        "-f",
    ]

    grep_cmd = (
        "grep",
        "-m1",
        '"Starting an election due to step up request"',
    )

    procs = []
    for unit in ops_test.model.applications[APP_NAME].units:
        kubectl_cmd[5] = unit.name.replace("/", "-")
        kubectl_proc = subprocess.Popen(kubectl_cmd, stdout=subprocess.PIPE)
        grep_proc = subprocess.Popen(
            grep_cmd, stdin=kubectl_proc.stdout, stdout=subprocess.DEVNULL
        )
        procs.append((kubectl_proc, grep_proc))

    timeout = 30
    success = False
    for kubectl_proc, grep_proc in procs:
        try:
            grep_proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            pass
        if grep_proc.poll() == 0:
            success = True
        grep_proc.terminate()
        kubectl_proc.terminate()
        timeout = 0

    assert success, "old primary departed without stepping down."


async def set_log_level(ops_test: OpsTest, level: int, component: str = None) -> None:
    """Sets a given loglevel for a given component for each mongodb unit."""
    pass_unit = ops_test.model.applications[APP_NAME].units[0].name
    cmd = [
        "ssh",
        "--container",
        MONGODB_CONTAINER_NAME,
        "",
        "mongosh",
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
