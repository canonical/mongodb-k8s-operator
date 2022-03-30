import logging

from typing import List
from pymongo import MongoClient
from dataclasses import dataclass
from urllib.parse import quote_plus
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
    hosts: List[str]
    sharding: bool


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
            connectTimeoutMS=10000,
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
            # The ping command is cheap and does not require auth.
            self.client.admin.command("ping")
        except PyMongoError:
            return False

        return True

    def init_replset(self):
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
                logger.error("cannot initialize replica set. error=%r", e)
                raise e

    @property
    def is_replset_up_to_date(self) -> bool:
        """Is the replica set has all members?

        Returns:
            True if all peer members included in MongoDB status output.
        """
        status = self.client.admin.command("replSetGetStatus")
        curr_members = [
            str(member["name"]).split(":")[0] for member in status["members"]
        ]
        return set(self.mongodb_config.hosts) == set(curr_members)

    def reconfigure_replset(self):
        """Reconfigure replica set membership inside MongoDB
        according to current peer membership
        """
        pass
