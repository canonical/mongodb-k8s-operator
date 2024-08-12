#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import json

from pytest_operator.plugin import OpsTest
from tenacity import (
    RetryError,
    Retrying,
    retry,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

from ..helpers import get_application_relation_data


async def verify_application_data(
    ops_test: OpsTest,
    application_name: str,
    database_app: str,
    relation_name: str,
) -> bool:
    """Verifies the application relation metadata matches with the MongoDB deployment.

    Specifically, it verifies that all units are present in the URI and that there are no
    additional units
    """
    try:
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                endpoints_str = await get_application_relation_data(
                    ops_test, application_name, relation_name, "endpoints"
                )
                for unit in ops_test.model.applications[database_app].units:
                    if unit.public_address not in endpoints_str:
                        raise Exception(f"unit {unit.name} not present in connection URI")

                if len(endpoints_str.split(",")) != len(
                    ops_test.model.applications[database_app].units
                ):
                    raise Exception(
                        "number of endpoints in replicaset URI do not match number of units"
                    )

    except RetryError:
        return False

    return True


async def get_secret_data(ops_test, secret_uri):
    secret_unique_id = secret_uri.split("/")[-1]
    complete_command = f"show-secret {secret_uri} --reveal --format=json"
    _, stdout, _ = await ops_test.juju(*complete_command.split())
    return json.loads(stdout)[secret_unique_id]["content"]["Data"]


@retry(stop=stop_after_attempt(10), wait=wait_fixed(15), reraise=True)
async def get_connection_string(
    ops_test: OpsTest, app_name, relation_name, relation_id=None, relation_alias=None
) -> str:
    secret_uri = await get_application_relation_data(
        ops_test, app_name, relation_name, "secret-user", relation_id, relation_alias
    )
    assert secret_uri, "No secret URI found"

    first_relation_user_data = await get_secret_data(ops_test, secret_uri)
    return first_relation_user_data.get("uris")
