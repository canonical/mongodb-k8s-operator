#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
from typing import List, Optional


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
