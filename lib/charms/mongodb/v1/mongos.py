"""Code for interactions with MongoS."""

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple
from urllib.parse import quote_plus

from charms.mongodb.v1.mongodb import NotReadyError
from pymongo import MongoClient, collection
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

from config import Config

# The unique Charmhub library identifier, never change it
LIBID = "e20d5b19670d4c55a4934a21d3f3b29a"

# Increment this major API version when introducing breaking changes
LIBAPI = 1

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 6

# path to store mongodb ketFile
logger = logging.getLogger(__name__)

SHARD_AWARE_STATE = 1


@dataclass
class MongosConfiguration:
    """Class for mongos configuration.

    — database: database name.
    — username: username.
    — password: password.
    — hosts: full list of hosts to connect to, needed for the URI.
    - port: integer for the port to connect to connect to mongodb.
    - tls_external: indicator for use of internal TLS connection.
    - tls_internal: indicator for use of external TLS connection.
    """

    database: Optional[str]
    username: str
    password: str
    hosts: Set[str]
    port: int
    roles: Set[str]
    tls_external: bool
    tls_internal: bool

    @property
    def uri(self):
        """Return URI concatenated from fields."""
        self.complete_hosts = self.hosts

        # mongos using Unix Domain Socket to communicate do not use port
        if self.port:
            self.complete_hosts = [f"{host}:{self.port}" for host in self.hosts]

        complete_hosts = ",".join(self.complete_hosts)

        # Auth DB should be specified while user connects to application DB.
        auth_source = ""
        if self.database != "admin":
            auth_source = "authSource=admin"
        return (
            f"mongodb://{quote_plus(self.username)}:"
            f"{quote_plus(self.password)}@"
            f"{complete_hosts}/{quote_plus(self.database)}?"
            f"{auth_source}"
        )


class NotEnoughSpaceError(Exception):
    """Raised when there isn't enough space to movePrimary."""


class ShardNotInClusterError(Exception):
    """Raised when shard is not present in cluster, but it is expected to be."""


class ShardNotPlannedForRemovalError(Exception):
    """Raised when it is expected that a shard is planned for removal, but it is not."""


class NotDrainedError(Exception):
    """Raised when a shard is still being drained."""


class BalancerNotEnabledError(Exception):
    """Raised when balancer process is not enabled."""


class MongosConnection:
    """In this class we create connection object to Mongos.

    Real connection is created on the first call to Mongos.
    Delayed connectivity allows to firstly check database readiness
    and reuse the same connection for an actual query later in the code.

    Connection is automatically closed when object destroyed.
    Automatic close allows to have more clean code.

    Note that connection when used may lead to the following pymongo errors: ConfigurationError,
    ConfigurationError, OperationFailure. It is suggested that the following pattern be adopted
    when using MongoDBConnection:

    with MongoMongos(self._mongos_config) as mongo:
        try:
            mongo.<some operation from this class>
        except ConfigurationError, OperationFailure:
            <error handling as needed>
    """

    def __init__(self, config: MongosConfiguration, uri=None, direct=False):
        """A MongoDB client interface.

        Args:
            config: MongoDB Configuration object.
            uri: allow using custom MongoDB URI, needed for replSet init.
            direct: force a direct connection to a specific host, avoiding
                    reading replica set configuration and reconnection.
        """
        if uri is None:
            uri = config.uri

        self.client = MongoClient(
            uri,
            directConnection=direct,
            connect=False,
            serverSelectionTimeoutMS=1000,
            connectTimeoutMS=2000,
        )
        return

    def __enter__(self):
        """Return a reference to the new connection."""
        return self

    def __exit__(self, object_type, value, traceback):
        """Disconnect from MongoDB client."""
        self.client.close()
        self.client = None

    def get_shard_members(self) -> Set[str]:
        """Gets shard members.

        Returns:
            A set of the shard members as reported by mongos.

        Raises:
            ConfigurationError, OperationFailure
        """
        shard_list = self.client.admin.command("listShards")
        curr_members = [
            self._hostname_from_hostport(member["host"]) for member in shard_list["shards"]
        ]
        return set(curr_members)

    def add_shard(self, shard_name, shard_hosts, shard_port=Config.MONGODB_PORT):
        """Adds shard to the cluster.

        Raises:
            ConfigurationError, OperationFailure
        """
        shard_hosts = [f"{host}:{shard_port}" for host in shard_hosts]
        shard_hosts = ",".join(shard_hosts)
        shard_url = f"{shard_name}/{shard_hosts}"
        if shard_name in self.get_shard_members():
            logger.info("Skipping adding shard %s, shard is already in cluster", shard_name)
            return

        logger.info("Adding shard %s", shard_name)
        self.client.admin.command("addShard", shard_url)

    def pre_remove_checks(self, shard_name):
        """Performs a series of checks for removing a shard from the cluster.

        Raises
            ConfigurationError, OperationFailure, NotReadyError, ShardNotInClusterError,
            BalencerNotEnabledError
        """
        if shard_name not in self.get_shard_members():
            logger.info("Shard to remove is not in cluster.")
            raise ShardNotInClusterError(f"Shard {shard_name} could not be removed")

        # It is necessary to call removeShard multiple times on a shard to guarantee removal.
        # Allow re-removal of shards that are currently draining.
        if self.is_any_draining(ignore_shard=shard_name):
            cannot_remove_shard = (
                f"cannot remove shard {shard_name} from cluster, another shard is draining"
            )
            logger.error(cannot_remove_shard)
            raise NotReadyError(cannot_remove_shard)

        # check if enabled sh.getBalancerState()
        balancer_state = self.client.admin.command("balancerStatus")
        if balancer_state["mode"] != "off":
            logger.info("Balancer is enabled, ready to remove shard.")
            return

        # starting the balancer doesn't guarantee that is is running, wait until it starts up.
        logger.info("Balancer process is not running, enabling it.")
        self.start_and_wait_for_balancer()

    def start_and_wait_for_balancer(self) -> None:
        """Turns on the balancer and waits for it to be running.

        Starting the balancer doesn't guarantee that is is running, wait until it starts up.

        Raises:
            BalancerNotEnabledError
        """
        self.client.admin.command("balancerStart")
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
            with attempt:
                balancer_state = self.client.admin.command("balancerStatus")
                if balancer_state["mode"] == "off":
                    raise BalancerNotEnabledError("balancer is not enabled.")

    def remove_shard(self, shard_name: str) -> None:
        """Removes shard from the cluster.

        Raises:
            ConfigurationError, OperationFailure, NotReadyError, NotEnoughSpaceError,
            ShardNotInClusterError, BalencerNotEnabledError
        """
        self.pre_remove_checks(shard_name)

        # remove shard, process removal status, & check if fully removed
        logger.info("Attempting to remove shard %s", shard_name)
        removal_info = self.client.admin.command("removeShard", shard_name)
        self._log_removal_info(removal_info, shard_name)
        remaining_chunks = self._retrieve_remaining_chunks(removal_info)
        if remaining_chunks:
            logger.info("Waiting for all chunks to be drained from %s.", shard_name)
            raise NotDrainedError()

        # MongoDB docs says to movePrimary only after all chunks have been drained from the shard.
        logger.info("All chunks drained from shard: %s", shard_name)
        databases_using_shard_as_primary = self.get_databases_for_shard(shard_name)
        if databases_using_shard_as_primary:
            logger.info(
                "These databases: %s use Shard %s is a primary shard, moving primary.",
                ", ".join(databases_using_shard_as_primary),
                shard_name,
            )
            self._move_primary(databases_using_shard_as_primary, old_primary=shard_name)

            # MongoDB docs says to re-run removeShard after running movePrimary
            logger.info("removing shard: %s, after moving primary", shard_name)
            removal_info = self.client.admin.command("removeShard", shard_name)
            self._log_removal_info(removal_info, shard_name)

        if shard_name in self.get_shard_members():
            logger.info("Shard %s is still present in sharded cluster.", shard_name)
            raise NotDrainedError()

    def _is_shard_draining(self, shard_name: str) -> bool:
        """Reports if a given shard is currently in the draining state.

        Raises:
            ConfigurationError, OperationFailure, ShardNotInClusterError,
            ShardNotPlannedForRemovalError
        """
        sc_status = self.client.admin.command("listShards")
        for shard in sc_status["shards"]:
            if shard["_id"] == shard_name:
                if "draining" not in shard:
                    raise ShardNotPlannedForRemovalError(
                        f"Shard {shard_name} has not been marked for removal",
                    )
                return shard["draining"]

        raise ShardNotInClusterError(
            f"Shard {shard_name} not in cluster, could not retrieve draining status"
        )

    def get_databases_for_shard(self, primary_shard) -> Optional[List[str]]:
        """Returns a list of databases using the given shard as a primary shard.

        In Sharded MongoDB clusters, mongos selects the primary shard when creating a new database
        by picking the shard in the cluster that has the least amount of data. This means that:
        1. There can be multiple primary shards in a cluster.
        2. Until there is data written to the cluster there is effectively no primary shard.
        """
        databases_collection = self._get_databases_collection()
        if databases_collection is None:
            return

        return databases_collection.distinct("_id", {"primary": primary_shard})

    def _get_databases_collection(self) -> collection.Collection:
        """Returns the databases collection if present.

        The collection `databases` only gets created once data is written to the sharded cluster.
        """
        config_db = self.client["config"]
        if "databases" not in config_db.list_collection_names():
            logger.info("No data written to sharded cluster yet.")
            return None

        return config_db["databases"]

    def is_any_draining(self, ignore_shard: str = "") -> bool:
        """Returns true if any shard members is draining.

        Checks if any members in sharded cluster are draining data.

        Args:
            sc_status: current state of shard cluster status as reported by mongos.
            ignore_shard: shard to ignore
        """
        sc_status = self.client.admin.command("listShards")
        return any(
            # check draining status of all shards except the one to be ignored.
            shard.get("draining", False) if shard["_id"] != ignore_shard else False
            for shard in sc_status["shards"]
        )

    @staticmethod
    def _hostname_from_hostport(hostname: str) -> str:
        """Return hostname part from MongoDB returned.

        mongos typically returns a value that contains both, hostname, hosts, and ports.
        e.g. input: shard03/host7:27018,host8:27018,host9:27018
        Return shard name
        e.g. output: shard03
        """
        return hostname.split("/")[0]

    def _log_removal_info(self, removal_info, shard_name):
        """Logs removal information for a shard removal."""
        remaining_chunks = self._retrieve_remaining_chunks(removal_info)
        dbs_to_move = (
            removal_info["dbsToMove"]
            if "dbsToMove" in removal_info and removal_info["dbsToMove"] != []
            else ["None"]
        )
        logger.info(
            "Shard %s is draining status is: %s. Remaining chunks: %s. DBs to move: %s.",
            shard_name,
            removal_info["state"],
            str(remaining_chunks),
            ",".join(dbs_to_move),
        )

    @property
    def is_ready(self) -> bool:
        """Is mongos ready for services requests.

        Returns:
            True if services is ready False otherwise. Retries over a period of 60 seconds times to
            allow server time to start up.

        Raises:
            ConfigurationError, ConfigurationError, OperationFailure
        """
        try:
            for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
                with attempt:
                    # The ping command is cheap and does not require auth.
                    self.client.admin.command("ping")
        except RetryError:
            return False

        return True

    def are_all_shards_aware(self) -> bool:
        """Returns True if all shards are shard aware."""
        sc_status = self.client.admin.command("listShards")
        for shard in sc_status["shards"]:
            if shard["state"] != SHARD_AWARE_STATE:
                return False

        return True

    def is_shard_aware(self, shard_name: str) -> bool:
        """Returns True if provided shard is shard aware."""
        sc_status = self.client.admin.command("listShards")
        for shard in sc_status["shards"]:
            if shard["_id"] == shard_name:
                return shard["state"] == SHARD_AWARE_STATE

        return False

    def _retrieve_remaining_chunks(self, removal_info) -> int:
        """Parses the remaining chunks to remove from removeShard command."""
        # when chunks have finished draining, remaining chunks is still in the removal info, but
        # marked as 0. If "remaining" is not present, in removal_info then the shard is not yet
        # draining
        if "remaining" not in removal_info:
            raise NotDrainedError()

        return removal_info["remaining"]["chunks"] if "remaining" in removal_info else 0

    def _move_primary(self, databases_to_move: List[str], old_primary: str) -> None:
        """Moves all the provided databases to a new primary.

        Raises:
            NotEnoughSpaceError, ConfigurationError, OperationFailure
        """
        for database_name in databases_to_move:
            db_size = self.get_db_size(database_name, old_primary)
            new_shard, avail_space = self.get_shard_with_most_available_space(
                shard_to_ignore=old_primary
            )
            if db_size > avail_space:
                no_space_on_new_primary = (
                    f"Cannot move primary for database: {database_name}, new shard: {new_shard}",
                    f"does not have enough space. {db_size} > {avail_space}",
                )
                logger.error(no_space_on_new_primary)
                raise NotEnoughSpaceError(no_space_on_new_primary)

            # From MongoDB Docs: After starting movePrimary, do not perform any read or write
            # operations against any unsharded collection in that database until the command
            # completes.
            logger.info(
                "Moving primary on %s database to new primary: %s. Do NOT write to %s database.",
                database_name,
                new_shard,
                database_name,
            )
            # This command does not return until MongoDB completes moving all data. This can take
            # a long time.
            self.client.admin.command("movePrimary", database_name, to=new_shard)
            logger.info(
                "Successfully moved primary on %s database to new primary: %s",
                database_name,
                new_shard,
            )

    def get_db_size(self, database_name, primary_shard) -> int:
        """Returns the size of a DB on a given shard in bytes."""
        database = self.client[database_name]
        db_stats = database.command("dbStats")

        # sharded databases are spread across multiple shards, find the amount of storage used on
        # the primary shard
        for shard_name, shard_storage_info in db_stats["raw"].items():
            # shard names are of the format `shard-one/10.61.64.212:27017`
            shard_name = shard_name.split("/")[0]
            if shard_name != primary_shard:
                continue

            return shard_storage_info["storageSize"]

        return 0

    def get_shard_with_most_available_space(self, shard_to_ignore) -> Tuple[str, int]:
        """Returns the shard in the cluster with the most available space and the space in bytes.

        Algorithm used was similar to that used in mongo in `selectShardForNewDatabase`:
        https://github.com/mongodb/mongo/blob/6/0/src/mongo/db/s/config/sharding_catalog_manager_database_operations.cpp#L68-L91
        """
        candidate_shard = None
        candidate_free_space = -1
        available_storage = self.client.admin.command("dbStats", freeStorage=1)

        for shard_name, shard_storage_info in available_storage["raw"].items():
            # shard names are of the format `shard-one/10.61.64.212:27017`
            shard_name = shard_name.split("/")[0]
            if shard_name == shard_to_ignore:
                continue

            current_free_space = shard_storage_info["freeStorageSize"]
            if current_free_space > candidate_free_space:
                candidate_shard = shard_name
                candidate_free_space = current_free_space

        return (candidate_shard, candidate_free_space)

    def get_draining_shards(self) -> List[str]:
        """Returns a list of the shards currently draining."""
        sc_status = self.client.admin.command("listShards")
        draining_shards = []
        for shard in sc_status["shards"]:
            if shard.get("draining", False):
                draining_shards.append(shard["_id"])

        return draining_shards
