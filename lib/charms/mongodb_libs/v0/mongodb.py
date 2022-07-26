"""Code for interactions with MongoDB."""
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import re
import logging
from dataclasses import dataclass
from typing import Set, Dict, List, Optional
from urllib.parse import quote_plus
from bson.json_util import dumps

from tenacity import (
    retry,
    stop_after_delay,
    stop_after_attempt,
    wait_fixed,
    before_log,
    RetryError,
    Retrying,
)

from pymongo import MongoClient
from pymongo.errors import (
    OperationFailure,
    PyMongoError,
)

# The unique Charmhub library identifier, never change it
LIBID = "1057f353503741a98ed79309b5be7e30"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version.
LIBPATCH = 0

# path to store mongodb ketFile
logger = logging.getLogger(__name__)


@dataclass
class MongoDBConfiguration:
    """
    Class for MongoDB configuration:
    — replset: name of replica set, needed for connection URI.
    — database: database name.
    — username: username.
    — password: password.
    — hosts: full list of hosts to connect to, needed for the URI.
    """

    replset: str
    database: Optional[str]
    username: str
    password: str
    hosts: Set[str]
    roles: Set[str]
    tls: bool

    @property
    def uri(self):
        """Return URI concatenated from fields."""
        hosts = ",".join(self.hosts)
        # Auth DB should be specified while user connects to application DB.
        auth_source = ""
        if self.database != "admin":
            auth_source = "&authSource=admin"
        return (
            f"mongodb://{quote_plus(self.username)}:"
            f"{quote_plus(self.password)}@"
            f"{hosts}/{quote_plus(self.database)}?"
            f"replicaSet={quote_plus(self.replset)}"
            f"{auth_source}"
        )


class NotReadyError(PyMongoError):
    """Raised when not all replica set members healthy or finished initial sync."""


class MongoDBConnection:
    """
    In this class we create connection object to MongoDB.

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

    def __init__(self, config: MongoDBConfiguration, uri=None, direct=False):
        """A MongoDB client interface.

        Args:
            — config: MongoDB Configuration object.
            — uri: allow using custom MongoDB URI, needed for replSet init.
            — direct: force a direct connection to a specific host, avoiding
                    reading replica set configuration and reconnection.
        """
        self.mongodb_config = config

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
        return self

    def __exit__(self, object_type, value, traceback):
        self.client.close()
        self.client = None

    @property
    def is_ready(self) -> bool:
        """Is the MongoDB server ready for services requests.

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def init_replset(self) -> None:
        """Create replica set config the first time

        Raises:
            ConfigurationError, ConfigurationError, OperationFailure
        """
        config = {
            "_id": self.mongodb_config.replset,
            "members": [
                {"_id": i, "host": h}
                for i, h in enumerate(self.mongodb_config.hosts)
            ],
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
        if self._is_any_sync(rs_status):
            # it can take a while, we should defer
            raise NotReadyError

        # Avoid reusing IDs, according to the doc
        # https://www.mongodb.com/docs/manual/reference/replica-configuration/
        max_id = max([
            int(member["_id"]) for member in rs_config["config"]["members"]
        ])
        new_member = {"_id": int(max_id + 1), "host": hostname}

        rs_config["config"]["version"] += 1
        rs_config["config"]["members"].extend([new_member])
        logger.debug("rs_config: %r", rs_config["config"])
        self.client.admin.command("replSetReconfig", rs_config["config"])

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
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
            # removing from replicaset is fast operation, lets @retry(3 times with a 5sec timeout) before giving up.
            raise NotReadyError

        # avoid downtime we need to reelect new primary
        # if removable member is the primary.
        logger.debug("primary: %r", self._is_primary(rs_status, hostname))
        if self._is_primary(rs_status, hostname):
            self.client.admin.command("replSetStepDown", {"stepDownSecs": "60"})

        rs_config["config"]["version"] += 1
        rs_config["config"]["members"][:] = [
            member for member in rs_config["config"]["members"]
            if hostname != self._hostname_from_hostport(member["host"])
        ]
        logger.debug("rs_config: %r", dumps(rs_config["config"]))
        self.client.admin.command("replSetReconfig", rs_config["config"])

    def create_user(self, config: MongoDBConfiguration):
        """Create user.

        Grant read and write privileges for specified database.
        """
        self.client.admin.command(
            "createUser",
            config.username,
            pwd=config.password,
            roles=self._get_roles(config),
        )

    def update_user(self, config: MongoDBConfiguration):
        """Update grants on database."""
        self.client.admin.command(
            "updateUser",
            config.username,
            roles=self._get_roles(config),
        )

    def set_user_password(self, username, password: str):
        """Update the password."""
        self.client.admin.command(
            "updateUser",
            username,
            pwd=password,
        )

    @staticmethod
    def _get_roles(config: MongoDBConfiguration) -> List[dict]:
        """Generate roles List."""
        supported_roles = {
            "admin": [
                {"role": "userAdminAnyDatabase", "db": "admin"},
                {"role": "readWriteAnyDatabase", "db": "admin"},
                {"role": "userAdmin", "db": "admin"},
            ],
            "default": [
                {"role": "readWrite", "db": config.database},
            ],
        }
        return [
            role_dict
            for role in config.roles
            for role_dict in supported_roles[role]
        ]

    def drop_user(self, username: str):
        """Drop user"""
        self.client.admin.command("dropUser", username)

    def get_users(self) -> Set[str]:
        """Add a new member to replica set config inside MongoDB."""
        users_info = self.client.admin.command("usersInfo")
        return set([
            user_obj["user"]
            for user_obj in users_info["users"]
            if re.match(r"^relation-\d+$", user_obj["user"])
        ])

    def get_databases(self) -> Set[str]:
        """Return list of all non-default databases."""
        system_dbs = ("admin", "local", "config")
        databases = self.client.list_database_names()
        return set([
            db for db in databases if db not in system_dbs
        ])

    def drop_database(self, database: str):
        """Drop a non-default database."""
        system_dbs = ("admin", "local", "config")
        if database in system_dbs:
            return
        self.client.drop_database(database)

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

    @staticmethod
    def _is_any_sync(rs_status: Dict) -> bool:
        """Returns true if any replica set members are syncing data.

        Checks if any members in replica set are syncing data. Note it is recommended to run only
        one sync in the cluster to not have huge performance degradation.

        Args:
            rs_status: current state of replica set as reported by mongod.
        """
        return any(
            member["stateStr"] == "STARTUP"
            or member["stateStr"] == "STARTUP2"
            or member["stateStr"] == "ROLLBACK"
            or member["stateStr"] == "RECOVERING"
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
        return any(
            member["stateStr"] == "REMOVED"
            for member in rs_status["members"]
        )

    @staticmethod
    def _hostname_from_hostport(hostname: str) -> str:
        """Return hostname part from MongoDB returned.

        MongoDB typically returns a value that contains both, hostname and port.
        e.g. input: mongodb-1:27015
        Return hostname without changes if the port is not passed.
        e.g. input: mongodb-1
        """
        return hostname.split(":")[0]
