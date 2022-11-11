#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from datetime import datetime
from pathlib import Path

import ops
import yaml
from pytest_operator.plugin import OpsTest
from tenacity import RetryError, Retrying, stop_after_attempt, wait_exponential

from tests.integration.helpers import get_password

logger = logging.getLogger(__name__)

PORT = 27017
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


class ProcessError(Exception):
    """Raised when a process fails."""


async def check_tls(ops_test: OpsTest, unit: ops.model.Unit, enabled: bool) -> bool:
    """Returns whether TLS is enabled on the specific MongoDB instance.

    Args:
        ops_test: The ops test framework instance.
        unit: The unit to be checked.
        enabled: check if TLS is enabled/disabled

    Returns:
        Whether TLS is enabled/disabled.
    """
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt:
                return_code = await run_tls_check(ops_test, unit)
                tls_enabled = return_code == 0
                if enabled != tls_enabled:
                    raise ValueError(
                        f"TLS is{' not' if not tls_enabled else ''} enabled on {unit.name}"
                    )
                return True
    except RetryError:
        return False


def parse_hostname(hostname: str) -> str:
    """Parses a hostname to the corresponding endpoint version."""
    return hostname.replace("/", "-") + ".mongodb-k8s-endpoints"


async def run_tls_check(ops_test: OpsTest, unit: ops.model.Unit) -> int:
    """Returns the return code of the TLS check."""
    hosts = [parse_hostname(unit.name) for unit in ops_test.model.applications[APP_NAME].units]
    hosts = ",".join(hosts)
    password = await get_password(ops_test, unit_id=0)
    mongo_uri = f"mongodb://operator:{password}@{hosts}/admin?"

    mongo_cmd = (
        f"/usr/bin/mongosh {mongo_uri} --eval 'rs.status()'"
        f" --tls --tlsCAFile /etc/mongodb/external-ca.crt"
        f" --tlsCertificateKeyFile /etc/mongodb/external-cert.pem"
    )

    complete_command = f"ssh --container mongod {unit.name} {mongo_cmd}"
    ret_code, _, _ = await ops_test.juju(*complete_command.split())

    return ret_code


async def time_file_created(ops_test: OpsTest, unit_name: str, path: str) -> int:
    """Returns the unix timestamp of when a file was created on a specified unit."""
    time_cmd = f"ls -l --time-style=full-iso {path} "
    complete_command = f"ssh --container mongod {unit_name} {time_cmd}"
    return_code, ls_output, stderr = await ops_test.juju(*complete_command.split())

    if return_code != 0:
        logger.error(stderr)
        raise ProcessError(
            "Expected time command %s to succeed instead it failed: %s; %s",
            time_cmd,
            return_code,
            stderr,
        )

    return process_ls_time(ls_output)


async def time_process_started(ops_test: OpsTest, unit_name: str, process_name: str) -> int:
    """Retrieves the time that a given process started according to systemd."""
    logs = await run_command_on_unit(ops_test, unit_name, "/charm/bin/pebble changes")

    # find most recent start time. By parsing most recent logs (ie in reverse order)
    for log in reversed(logs.split("\n")):
        if "Replan" in log:
            return process_pebble_time(log.split()[4])

    raise Exception("Service was never started")


async def run_command_on_unit(ops_test: OpsTest, unit_name: str, command: str) -> str:
    """Run a command on a specific unit.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit to run the command on
        command: The command to run

    Returns:
        the command output if it succeeds, otherwise raises an exception.
    """
    complete_command = f"ssh --container mongod {unit_name} {command}"
    return_code, stdout, _ = await ops_test.juju(*complete_command.split())
    if return_code != 0:
        raise Exception(
            "Expected command %s to succeed instead it failed: %s", command, return_code
        )
    return stdout


def process_ls_time(ls_output):
    """Parse time representation as returned by the 'ls' command."""
    time_as_str = "T".join(ls_output.split("\n")[0].split(" ")[5:7])
    # further strip down additional milliseconds
    time_as_str = time_as_str[0:-3]
    d = datetime.strptime(time_as_str, "%Y-%m-%dT%H:%M:%S.%f")
    return d


def process_pebble_time(changes_output):
    """Parse time representation as returned by the 'pebble changes' command."""
    d = datetime.strptime(changes_output, "%H:%M")
    return d
