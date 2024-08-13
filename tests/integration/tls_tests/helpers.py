#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import ops
import yaml
from pytest_operator.plugin import OpsTest
from tenacity import RetryError, Retrying, stop_after_attempt, wait_exponential

from ..helpers import (
    get_app_name,
    get_application_relation_data,
    get_mongo_cmd,
    get_password,
    get_secret_content,
    get_secret_id,
)

logger = logging.getLogger(__name__)

PORT = 27017
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
EXTERNAL_CERT_PATH = "/etc/mongod/external-ca.crt"
INTERNAL_CERT_PATH = "/etc/mongod/internal-ca.crt"
TLS_RELATION_NAME = "certificates"


class ProcessError(Exception):
    """Raised when a process fails."""


async def check_tls(
    ops_test: OpsTest,
    unit: ops.model.Unit,
    enabled: bool,
    app_name: str | None = None,
    mongos: bool = False,
) -> bool:
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
                return_code = await run_tls_check(ops_test, unit, app_name, mongos=mongos)
                tls_enabled = return_code == 0
                if enabled != tls_enabled:
                    raise ValueError(
                        f"TLS is{' not' if not tls_enabled else ''} enabled on {unit.name}"
                    )
                return True
    except RetryError:
        return False


def parse_hostname(hostname: str, app_name: str) -> str:
    """Parses a hostname to the corresponding endpoint version."""
    unit_id = hostname.split("/")[1]
    return f"{app_name}-{unit_id}.{app_name}-endpoints"


async def run_tls_check(
    ops_test: OpsTest, unit: ops.model.Unit, app_name: str | None = None, mongos: bool = False
) -> int:
    """Returns the return code of the TLS check."""
    app_name = app_name or get_app_name(ops_test)
    port = "27017" if not mongos else "27018"
    hosts = [
        f"{parse_hostname(unit.name, app_name)}:{port}"
        for unit in ops_test.model.applications[app_name].units
    ]
    hosts = ",".join(hosts)
    password = await get_password(ops_test, unit_id=0, app_name=app_name)
    extra_args = f"?replicaSet={app_name}" if not mongos else ""
    mongo_uri = f"mongodb://operator:{password}@{hosts}/admin{extra_args}"

    mongo_cmd = await get_mongo_cmd(ops_test, unit.name)

    status_command = "rs.status()" if not mongos else "sh.status()"

    mongo_cmd += (
        f" {mongo_uri} --eval '{status_command}'"
        f" --tls --tlsCAFile /etc/mongod/external-ca.crt"
        f" --tlsCertificateKeyFile /etc/mongod/external-cert.pem"
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


async def scp_file_preserve_ctime(ops_test: OpsTest, unit_name: str, path: str) -> int:
    """Returns the unix timestamp of when a file was created on a specified unit."""
    # Retrieving the file
    filename = path.split("/")[-1]
    complete_command = f"scp --container mongod {unit_name}:{path} {filename}"
    return_code, _, stderr = await ops_test.juju(*complete_command.split())

    if return_code != 0:
        logger.error(stderr)
        raise ProcessError(
            "Expected command %s to succeed instead it failed: %s; %s",
            complete_command,
            return_code,
            stderr,
        )

    return f"{filename}"


async def get_file_content(ops_test: OpsTest, unit_name: str, path: str) -> str:
    filename = await scp_file_preserve_ctime(ops_test, unit_name, path)

    with open(filename, mode="r") as fd:
        return fd.read()


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
    return datetime.strptime(time_as_str, "%Y-%m-%dT%H:%M:%S.%f")


def process_pebble_time(changes_output):
    """Parse time representation as returned by the 'pebble changes' command."""
    return datetime.strptime(changes_output, "%H:%M")


async def check_certs_correctly_distributed(
    ops_test: OpsTest, unit: ops.Unit, app_name: str | None = None
) -> None:
    """Comparing expected vs distributed certificates.

    Verifying certificates downloaded on the charm against the ones distributed by the TLS operator
    """
    app_name = app_name or await get_app_name(ops_test)
    unit_secret_id = await get_secret_id(ops_test, unit.name)
    unit_secret_content = await get_secret_content(ops_test, unit_secret_id)

    # Get the values for certs from the relation, as provided by TLS Charm
    certificates_raw_data = await get_application_relation_data(
        ops_test, app_name, TLS_RELATION_NAME, "certificates"
    )
    certificates_data = json.loads(certificates_raw_data)

    # compare the TLS resources stored on the disk of the unit with the ones from the TLS relation
    for cert_type, cert_path in [("int", INTERNAL_CERT_PATH), ("ext", EXTERNAL_CERT_PATH)]:
        unit_csr = unit_secret_content[f"{cert_type}-csr-secret"]
        tls_item = [
            data
            for data in certificates_data
            if data["certificate_signing_request"].rstrip() == unit_csr.rstrip()
        ][0]

        # Read the content of the cert file stored in the unit
        cert_file_copy_path = await scp_file_preserve_ctime(ops_test, unit.name, cert_path)
        with open(cert_file_copy_path, mode="r") as f:
            cert_file_content = f.read()

        # cleanup the file
        os.remove(cert_file_copy_path)

        # Get the external cert value from the relation
        relation_cert = "\n".join(tls_item["chain"]).strip()

        # confirm that they match
        assert (
            relation_cert == cert_file_content
        ), f"Relation Content for {cert_type}-cert:\n{relation_cert}\nFile Content:\n{cert_file_content}\nMismatch."
