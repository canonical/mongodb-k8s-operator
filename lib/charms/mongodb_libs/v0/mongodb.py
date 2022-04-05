import logging
from dataclasses import dataclass
from typing import Set, Dict
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
# to 0 if you are raising the major API version
LIBPATCH = 0

# path to store mongodb ketFile
logger = logging.getLogger(__name__)


@dataclass
class MongoDBConfiguration:
    """
    Class for storing configuration for MongoDB
    - replset_name: name of replica set , needed for connection URI
    - admin_user: username used for administrative tasks
    - admin_password: password
    - hosts: full list of hosts to connect to, needed for URI
    - sharding: depending on sharding, cmd line and URI constructed differently
    """

    replset_name: str
    admin_user: str
    admin_password: str
    hosts: Set[str]
    sharding: bool


class NotReadyError(PyMongoError):
    """Raised when not all replica set members healthy or finished initial sync."""


class MongoDBConnection:
    """
    In this class we create connection object to MongoDB.

    Real connection is created on the first call to MongoDB.
    Delayed connectivity allows to firstly check database readiness
    and reuse the same connection for actual query later in the code.

    Connection is automatically closed when object destroyed.
    Automatic close allows to have more clean code.
    """

    def __init__(self, config: MongoDBConfiguration, uri=None, direct=False):
        """A MongoDB client interface.

        Args:
            config: MongoDB Configuration object
            uri: allow using custom MongoDB URI, needed for replSet init
            direct: force standalone connection, needed for replSet init
        """
        self.mongodb_config = config

        if uri is None:
            hosts = ",".join(config.hosts)
            uri = (
                f"mongodb://{quote_plus(config.admin_user)}:"
                f"{quote_plus(config.admin_password)}@"
                f"{hosts}/admin?"
                f"replicaSet={quote_plus(config.replset_name)}"
            )
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
            True if services is ready False otherwise.
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
        """Create replica set config the first time"""
        config = {
            "_id": self.mongodb_config.replset_name,
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
                #     created which is the step after this
                # AlreadyInitialized error can be raised only if this step
                #     finished
                logger.error("Cannot initialize replica set. error=%r", e)
                raise e

    @property
    def get_replset_members(self) -> Set[str]:
        """Is the replica set has all members?

        Returns:
            True if all peer members included in MongoDB status output.
        """
        rs_status = self.client.admin.command("replSetGetStatus")
        curr_members = [
            str(member["name"]).split(":")[0] for member in rs_status["members"]
        ]
        return set(curr_members)

    def add_replset_member(self, hostname: str) -> None:
        """Add new member to replica set config inside MongoDB"""
        rs_config = self.client.admin.command("replSetGetConfig")
        rs_status = self.client.admin.command("replSetGetStatus")

        # When we add new member, MongoDB transfer data from existing member to new.
        # Such operation reduce performance of the cluster. To avoid huge performance
        # degradation, before adding new members, it is needed to check that all other
        # members finished init sync.
        if self._is_any_sync(rs_status):
            # it can take a while, we should defer
            raise NotReadyError

        # Avoid reusing ids, according to the doc
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
        """Remove member from replica set config inside MongoDB"""
        rs_config = self.client.admin.command("replSetGetConfig")
        rs_status = self.client.admin.command("replSetGetStatus")

        # When we remove member, to avoid issues when majority removed, we need to
        # remove next member only when MongoDB forget the previous removed member.
        if self._is_any_removing(rs_status):
            # it is fast, we will @retry(3 times with a 5sec timeout) before giving up
            raise NotReadyError

        # avoid downtime we need to reelect new primary
        # if removable member is primary
        logger.debug("primary: %r", self._is_primary(rs_status, hostname))
        if self._is_primary(rs_status, hostname):
            self.client.admin.command("replSetStepDown", {"stepDownSecs": "60"})

        rs_config["config"]["version"] += 1
        rs_config["config"]["members"][:] = [
            member for member in rs_config["config"]["members"] if hostname != str(member["host"]).split(":")[0]
        ]
        logger.debug("rs_config: %r", dumps(rs_config["config"]))
        self.client.admin.command("replSetReconfig", rs_config["config"])

    @staticmethod
    def _is_primary(rs_status: Dict, hostname: str) -> bool:
        """Checking if passed member is primary"""
        return any(
            hostname == str(member["name"]).split(":")[0]
            and member["stateStr"] == "PRIMARY"
            for member in rs_status["members"]
        )

    @staticmethod
    def _is_any_sync(rs_status: Dict) -> bool:
        """Checking any members in replica set are syncing data right now
        It is recommended to run only one sync in cluster to not have huge
        performance degradation.
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
        """Checking any members in replica set are syncing data right now
        It is recommended to run only one sync in cluster to not have huge performance degradation
        """
        return any(
            member["stateStr"] == "REMOVED"
            for member in rs_status["members"]
        )
