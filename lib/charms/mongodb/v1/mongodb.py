"""Code for interactions with MongoDB."""

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import Dict, Set

from bson.json_util import dumps
from charms.mongodb.v0.mongo import MongoConfiguration, MongoConnection, NotReadyError
from pymongo.errors import OperationFailure
from tenacity import (
    RetryError,
    Retrying,
    before_log,
    retry,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

# The unique Charmhub library identifier, never change it
LIBID = "49c69d9977574dd7942eb7b54f43355b"

# Increment this major API version when introducing breaking changes
LIBAPI = 1

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 3

# path to store mongodb ketFile
logger = logging.getLogger(__name__)


class FailedToMovePrimaryError(Exception):
    """Raised when attempt to move a primary fails."""


class MongoDBConnection(MongoConnection):
    """In this class we create connection object to MongoDB.

    Real connection is created on the first call to MongoDB.
    Delayed connectivity allows to firstly check database readiness
    and reuse the same connection for an actual query later in the code.

    Connection is automatically closed when object destroyed.
    Automatic close allows to have more clean code.

    Note that connection when used may lead to the following pymongo errors: ConfigurationError,
    ConfigurationError, OperationFailure. It is suggested that the following pattern be adopted
    when using MongoDBConnection:

    with MongoDBConnection(self._mongodb_config) as mongo:
        try:
            mongo.<some operation from this class>
        except ConfigurationError, ConfigurationError, OperationFailure:
            <error handling as needed>
    """

    def __init__(self, config: MongoConfiguration, uri=None, direct=False):
        """A MongoDB client interface.

        Args:
            config: MongoDB Configuration object.
            uri: allow using custom MongoDB URI, needed for replSet init.
            direct: force a direct connection to a specific host, avoiding
                    reading replica set configuration and reconnection.
        """
        super().__init__(config, uri, direct)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def init_replset(self) -> None:
        """Create replica set config the first time.

        Raises:
            ConfigurationError, ConfigurationError, OperationFailure
        """
        config = {
            "_id": self.config.replset,
            "members": [{"_id": i, "host": h} for i, h in enumerate(self.config.hosts)],
        }
        try:
            self.client.admin.command("replSetInitiate", config)
        except OperationFailure as e:
            if e.code not in (13, 23):  # Unauthorized, AlreadyInitialized
                # Unauthorized error can be raised only if initial user were
                #     created the step after this.
                # AlreadyInitialized error can be raised only if this step
                #     finished.
                logger.error("Cannot initialize replica set. error=%r", e)
                raise e

    def get_replset_status(self) -> Dict:
        """Get a replica set status as a dict.

        Returns:
            A set of the replica set members along with their status.

        Raises:
            ConfigurationError, ConfigurationError, OperationFailure
        """
        rs_status = self.client.admin.command("replSetGetStatus")
        rs_status_parsed = {}
        for member in rs_status["members"]:
            member_name = self._hostname_from_hostport(member["name"])
            rs_status_parsed[member_name] = member["stateStr"]

        return rs_status_parsed

    def get_replset_members(self) -> Set[str]:
        """Get a replica set members.

        Returns:
            A set of the replica set members as reported by mongod.

        Raises:
            ConfigurationError, ConfigurationError, OperationFailure
        """
        rs_status = self.client.admin.command("replSetGetStatus")
        curr_members = [
            self._hostname_from_hostport(member["name"]) for member in rs_status["members"]
        ]
        return set(curr_members)

    def add_replset_member(self, hostname: str) -> None:
        """Add a new member to replica set config inside MongoDB.

        Raises:
            ConfigurationError, ConfigurationError, OperationFailure, NotReadyError
        """
        rs_config = self.client.admin.command("replSetGetConfig")
        rs_status = self.client.admin.command("replSetGetStatus")

        # When we add a new member, MongoDB transfer data from existing member to new.
        # Such operation reduce performance of the cluster. To avoid huge performance
        # degradation, before adding new members, it is needed to check that all other
        # members finished init sync.
        if self.is_any_sync(rs_status):
            # it can take a while, we should defer
            raise NotReadyError

        # Avoid reusing IDs, according to the doc
        # https://www.mongodb.com/docs/manual/reference/replica-configuration/
        max_id = max([int(member["_id"]) for member in rs_config["config"]["members"]])
        new_member = {"_id": int(max_id + 1), "host": hostname}

        rs_config["config"]["version"] += 1
        rs_config["config"]["members"].extend([new_member])
        logger.debug("rs_config: %r", rs_config["config"])
        self.client.admin.command("replSetReconfig", rs_config["config"])

    @retry(
        stop=stop_after_attempt(20),
        wait=wait_fixed(3),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def remove_replset_member(self, hostname: str) -> None:
        """Remove member from replica set config inside MongoDB.

        Raises:
            ConfigurationError, ConfigurationError, OperationFailure, NotReadyError
        """
        rs_config = self.client.admin.command("replSetGetConfig")
        rs_status = self.client.admin.command("replSetGetStatus")

        # When we remove member, to avoid issues when majority members is removed, we need to
        # remove next member only when MongoDB forget the previous removed member.
        if self._is_any_removing(rs_status):
            # removing from replicaset is fast operation, lets @retry(3 times with a 5sec timeout)
            # before giving up.
            raise NotReadyError

        # avoid downtime we need to reelect new primary if removable member is the primary.
        logger.debug("primary: %r", self._is_primary(rs_status, hostname))
        if self._is_primary(rs_status, hostname):
            self.client.admin.command("replSetStepDown", {"stepDownSecs": "60"})

        rs_config["config"]["version"] += 1
        rs_config["config"]["members"][:] = [
            member
            for member in rs_config["config"]["members"]
            if hostname != self._hostname_from_hostport(member["host"])
        ]
        logger.debug("rs_config: %r", dumps(rs_config["config"]))
        self.client.admin.command("replSetReconfig", rs_config["config"])

    def step_down_primary(self) -> None:
        """Steps down the current primary, forcing a re-election."""
        self.client.admin.command("replSetStepDown", {"stepDownSecs": "60"})

    def move_primary(self, new_primary_ip: str) -> None:
        """Forcibly moves the primary to the new primary provided.

        Args:
            new_primary_ip: ip address of the unit chosen to be the new primary.
        """
        # Do not move a priary unless the cluster is in sync
        rs_status = self.client.admin.command("replSetGetStatus")
        if self.is_any_sync(rs_status):
            # it can take a while, we should defer
            raise NotReadyError

        is_move_successful = True
        self.set_replicaset_election_priority(priority=0.5, ignore_member=new_primary_ip)
        try:
            for attempt in Retrying(stop=stop_after_delay(180), wait=wait_fixed(3)):
                with attempt:
                    self.step_down_primary()
                    if self.primary() != new_primary_ip:
                        raise FailedToMovePrimaryError
        except RetryError:
            # catch all possible exceptions when failing to step down primary. We do this in order
            # to ensure that we reset the replica set election priority.
            is_move_successful = False

        # reset all replicas to the same priority
        self.set_replicaset_election_priority(priority=1)

        if not is_move_successful:
            raise FailedToMovePrimaryError

    def set_replicaset_election_priority(self, priority: int, ignore_member: str = None) -> None:
        """Set the election priority for the entire replica set."""
        rs_config = self.client.admin.command("replSetGetConfig")
        rs_config = rs_config["config"]
        rs_config["version"] += 1

        # keep track of the original configuration before setting the priority, reconfiguring the
        # replica set can result in primary re-election, which would would like to avoid when
        # possible.
        original_rs_config = rs_config

        for member in rs_config["members"]:
            if member["host"] == ignore_member:
                continue

            member["priority"] = priority

        if original_rs_config == rs_config:
            return

        logger.debug("rs_config: %r", rs_config)
        self.client.admin.command("replSetReconfig", rs_config)

    def _is_primary(self, rs_status: Dict, hostname: str) -> bool:
        """Returns True if passed host is the replica set primary.

        Args:
            hostname: host of interest.
            rs_status: current state of replica set as reported by mongod.
        """
        return any(
            hostname == self._hostname_from_hostport(member["name"])
            and member["stateStr"] == "PRIMARY"
            for member in rs_status["members"]
        )

    def primary(self) -> str:
        """Returns primary replica host."""
        status = self.client.admin.command("replSetGetStatus")

        primary = None
        # loop through all members in the replica set
        for member in status["members"]:
            # check replica's current state
            if member["stateStr"] == "PRIMARY":
                primary = self._hostname_from_hostport(member["name"])

        return primary

    @staticmethod
    def is_any_sync(rs_status: Dict) -> bool:
        """Returns true if any replica set members are syncing data.

        Checks if any members in replica set are syncing data. Note it is recommended to run only
        one sync in the cluster to not have huge performance degradation.

        Args:
            rs_status: current state of replica set as reported by mongod.
        """
        return any(
            member["stateStr"] in ["STARTUP", "STARTUP2", "ROLLBACK", "RECOVERING"]
            for member in rs_status["members"]
        )

    @staticmethod
    def _is_any_removing(rs_status: Dict) -> bool:
        """Returns true if any replica set members are removing now.

        Checks if any members in replica set are getting removed. It is recommended to run only one
        removal in the cluster at a time as to not have huge performance degradation.

        Args:
            rs_status: current state of replica set as reported by mongod.
        """
        return any(member["stateStr"] == "REMOVED" for member in rs_status["members"])

    @staticmethod
    def _hostname_from_hostport(hostname: str) -> str:
        """Return hostname part from MongoDB returned.

        MongoDB typically returns a value that contains both, hostname and port.
        e.g. input: mongodb-1:27015
        Return hostname without changes if the port is not passed.
        e.g. input: mongodb-1
        """
        return hostname.split(":")[0]
