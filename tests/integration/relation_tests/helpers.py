#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import json
from typing import Optional

import yaml
from pytest_operator.plugin import OpsTest
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed
from urllib.parse import urlparse, parse_qs


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

def get_info_from_mongo_connection_string(connection_string: str) -> dict:
    parsed_url = urlparse(connection_string)

    netloc = parsed_url.netloc

    path = parsed_url.path.lstrip('/')  # Remove leading slash to get the database name

    query_dict = parse_qs(parsed_url.query)

    user_info, hosts = netloc.split('@')

    username, password = user_info.split(':')

    host_list = hosts.split(',')

    replica_set = query_dict.get('replicaSet', [None])[0]

    return {
        "username": username,
        "password": password,
        "hosts": host_list,
        "database": path,
        "replicaSet": replica_set
    }
