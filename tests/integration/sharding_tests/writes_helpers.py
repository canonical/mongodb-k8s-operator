# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from typing import Dict, List

import yaml
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest

from ..helpers import MONGOS_PORT, mongodb_uri

# TODO move these to a separate file for constants \ config
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = "config-server"
APP_NAME_NEW = "config-server-new"

logger = logging.getLogger(__name__)

SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
CONFIG_SERVER_APP_NAME = "config-server"

DEFAULT_DB_NAME = "new-db"
DEFAULT_COLL_NAME = "test_collection"
SHARD_ONE_DB_NAME = f"{SHARD_ONE_APP_NAME}_{DEFAULT_DB_NAME}".replace("-", "_")
SHARD_TWO_DB_NAME = f"{SHARD_TWO_APP_NAME}_{DEFAULT_DB_NAME}".replace("-", "_")


class ProcessError(Exception):
    """Raised when a process fails."""


class ProcessRunningError(Exception):
    """Raised when a process is running when it is not expected to be."""


async def remove_db_writes(
    ops_test: OpsTest,
    db_name: str,
    coll_name: str = DEFAULT_COLL_NAME,
) -> bool:
    """Stop the DB process and remove any writes to the test collection."""
    # remove collection from database
    config_server_name = APP_NAME if APP_NAME in ops_test.model.applications else APP_NAME_NEW
    connection_string = await mongodb_uri(ops_test, app_name=config_server_name, port=MONGOS_PORT)

    client = MongoClient(connection_string)
    db = client[db_name]

    # collection for continuous writes
    test_collection = db[coll_name]
    test_collection.drop()

    client.close()


async def count_shard_writes(
    ops_test: OpsTest,
    config_server_name=CONFIG_SERVER_APP_NAME,
    db_name="new-db",
    collection_name=DEFAULT_COLL_NAME,
) -> int:
    """New versions of pymongo no longer support the count operation, instead find is used."""
    connection_string = await mongodb_uri(ops_test, app_name=config_server_name, port=MONGOS_PORT)

    client = MongoClient(connection_string)
    db = client[db_name]
    test_collection = db[collection_name]
    count = test_collection.count_documents({})
    client.close()
    return count


async def get_cluster_writes_count(
    ops_test,
    shard_app_names: List[str],
    db_names: List[str],
    config_server_name: str = CONFIG_SERVER_APP_NAME,
) -> Dict:
    """Returns a dictionary of the writes for each cluster_component and the total writes."""
    cluster_write_count = {}
    total_writes = 0
    for app_name in shard_app_names:
        cluster_write_count[app_name] = 0
        for db in db_names:
            component_writes = await count_shard_writes(ops_test, config_server_name, db_name=db)
            cluster_write_count[app_name] += component_writes
            total_writes += component_writes

    cluster_write_count["total_writes"] = total_writes
    return cluster_write_count


async def insert_unwanted_data(ops_test: OpsTest, config_server_name=APP_NAME) -> None:
    """Inserts the data into the MongoDB cluster via primary replica."""
    connection_string = await mongodb_uri(ops_test, app_name=config_server_name, port=MONGOS_PORT)

    client = MongoClient(connection_string)
    db = client["new-db"]
    test_collection = db["test_collection"]
    test_collection.insert_one({"unwanted_data": "bad data 1"})
    test_collection.insert_one({"unwanted_data": "bad data 2"})
    test_collection.insert_one({"unwanted_data": "bad data 3"})
    client.close()


def write_data_to_mongodb(client, db_name, coll_name, content) -> None:
    """Writes data to the provided collection and database."""
    db = client[db_name]
    horses_collection = db[coll_name]
    horses_collection.insert_one(content)
