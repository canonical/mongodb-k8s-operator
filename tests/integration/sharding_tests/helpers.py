#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
from typing import List, Optional, Tuple

from pymongo import MongoClient
from pytest_operator.plugin import OpsTest
from tenacity import retry, stop_after_attempt, wait_fixed

from ..helpers import METADATA, get_application_relation_data, get_secret_content

SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
CONFIG_SERVER_APP_NAME = "config-server"
CONFIG_SERVER_REL_NAME = "config-server"
MONGODB_CHARM_NAME = "mongodb-k8s"
SHARD_REL_NAME = "sharding"
CLUSTER_COMPONENTS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, CONFIG_SERVER_APP_NAME]
TIMEOUT = 15 * 60
MONGOS_PORT = 27018
MONGOD_PORT = 27017


def count_users(mongos_client: MongoClient) -> int:
    """Returns the number of users using the cluster."""
    admin_db = mongos_client["admin"]
    users_collection = admin_db.system.users
    return users_collection.count_documents({})


@retry(stop=stop_after_attempt(10), wait=wait_fixed(15), reraise=True)
async def get_related_username_password(
    ops_test: OpsTest, app_name: str, relation_name: str
) -> Tuple:
    """Retrieves the credentials for an integrated application using app_name and relation_name."""
    secret_uri = await get_application_relation_data(
        ops_test, app_name, relation_name, "secret-user"
    )
    relation_user_data = await get_secret_content(ops_test, secret_uri)
    username = relation_user_data.get("username")
    password = relation_user_data.get("password")
    return (username, password)


async def deploy_cluster_components(
    ops_test: OpsTest, num_units_cluster_config: dict | None = None, channel: str | None = None
) -> None:
    if not num_units_cluster_config:
        num_units_cluster_config = {
            CONFIG_SERVER_APP_NAME: 2,
            SHARD_ONE_APP_NAME: 3,
            SHARD_TWO_APP_NAME: 1,
        }

    if channel is None:
        my_charm = await ops_test.build_charm(".")
    else:
        my_charm = MONGODB_CHARM_NAME

    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        num_units=num_units_cluster_config[CONFIG_SERVER_APP_NAME],
        config={"role": "config-server"},
        application_name=CONFIG_SERVER_APP_NAME,
        channel=channel,
        series="jammy",
        trust=True,
    )
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        num_units=num_units_cluster_config[SHARD_ONE_APP_NAME],
        config={"role": "shard"},
        application_name=SHARD_ONE_APP_NAME,
        channel=channel,
        series="jammy",
        trust=True,
    )
    await ops_test.model.deploy(
        my_charm,
        resources=resources,
        num_units=num_units_cluster_config[SHARD_TWO_APP_NAME],
        config={"role": "shard"},
        application_name=SHARD_TWO_APP_NAME,
        channel=channel,
        series="jammy",
        trust=True,
    )

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_blocked=False,
        raise_on_error=False,
    )


async def integrate_cluster(ops_test: OpsTest) -> None:
    """Integrates the cluster components with each other."""
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )
    await ops_test.model.integrate(
        f"{SHARD_TWO_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=15,
        status="active",
    )


def get_cluster_shards(mongos_client) -> set:
    """Returns a set of the shard members."""
    shard_list = mongos_client.admin.command("listShards")
    curr_members = [member["host"].split("/")[0] for member in shard_list["shards"]]
    return set(curr_members)


def has_correct_shards(mongos_client, expected_shards: List[str]) -> bool:
    """Returns true if the cluster config has the expected shards."""
    shard_names = get_cluster_shards(mongos_client)
    return shard_names == set(expected_shards)


def shard_has_databases(
    mongos_client, shard_name: str, expected_databases_on_shard: List[str]
) -> bool:
    """Returns true if the provided shard is a primary for the provided databases."""
    databases_on_shard = get_databases_for_shard(mongos_client, shard_name=shard_name)
    return set(databases_on_shard) == set(expected_databases_on_shard)


def get_databases_for_shard(mongos_client, shard_name) -> Optional[List[str]]:
    """Returns the databases hosted on the given shard."""
    config_db = mongos_client["config"]
    if "databases" not in config_db.list_collection_names():
        return None

    databases_collection = config_db["databases"]

    if databases_collection is None:
        return

    return databases_collection.distinct("_id", {"primary": shard_name})


def write_data_to_mongodb(client, db_name, coll_name, content) -> None:
    """Writes data to the provided collection and database."""
    db = client[db_name]
    test_collection = db[coll_name]
    test_collection.insert_one(content)


def verify_data_mongodb(client, db_name, coll_name, key, value) -> bool:
    """Checks a key/value pair for a provided collection and database."""
    db = client[db_name]
    test_collection = db[coll_name]
    query = test_collection.find({}, {key: 1})
    return query[0][key] == value
