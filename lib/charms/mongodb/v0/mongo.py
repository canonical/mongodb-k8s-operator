"""Code for interactions with MongoDB."""

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import re
from dataclasses import dataclass
from itertools import chain
from typing import List, Set
from urllib.parse import quote_plus

from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

from config import Config

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


class NotReadyError(PyMongoError):
    """Raised when not all replica set members healthy or finished initial sync."""


# The unique Charmhub library identifier, never change it
LIBID = "3037662a76cc4bf1876d4659c88e77e5"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

ADMIN_AUTH_SOURCE = "authSource=admin"
SYSTEM_DBS = ("admin", "local", "config")
REGULAR_ROLES = {
    "admin": [
        {"role": "userAdminAnyDatabase", "db": "admin"},
        {"role": "readWriteAnyDatabase", "db": "admin"},
        {"role": "userAdmin", "db": "admin"},
    ],
    "monitor": [
        {"role": "explainRole", "db": "admin"},
        {"role": "clusterMonitor", "db": "admin"},
        {"role": "read", "db": "local"},
    ],
    "backup": [
        {"db": "admin", "role": "readWrite", "collection": ""},
        {"db": "admin", "role": "backup"},
        {"db": "admin", "role": "clusterMonitor"},
        {"db": "admin", "role": "restore"},
        {"db": "admin", "role": "pbmAnyAction"},
    ],
}

# path to store mongodb ketFile
logger = logging.getLogger(__name__)


class AmbiguousConfigError(Exception):
    """Raised when the config could correspond to a mongod config or mongos config."""


@dataclass
class MongoConfiguration:
    """Class for Mongo configurations usable my mongos and mongodb.

    — replset: name of replica set
    — database: database name.
    — username: username.
    — password: password.
    — hosts: full list of hosts to connect to, needed for the URI.
    — tls_external: indicator for use of internal TLS connection.
    — tls_internal: indicator for use of external TLS connection.
    """

    database: str | None
    username: str
    password: str
    hosts: Set[str]
    roles: Set[str]
    tls_external: bool
    tls_internal: bool
    port: int = Config.MONGODB_PORT
    replset: str | None = None
    standalone: bool = False

    @property
    def uri(self):
        """Return URI concatenated from fields."""
        if self.port == Config.MONGOS_PORT and self.replset:
            raise AmbiguousConfigError("Mongos cannot support replica set")

        if self.standalone:
            return (
                f"mongodb://{quote_plus(self.username)}:"
                f"{quote_plus(self.password)}@"
                f"localhost:{self.port}/?authSource=admin"
            )

        self.complete_hosts = self.hosts

        # mongos using Unix Domain Socket to communicate do not use port
        if self.port:
            self.complete_hosts = [f"{host}:{self.port}" for host in self.hosts]

        complete_hosts = ",".join(self.complete_hosts)

        replset_str = f"replicaSet={quote_plus(self.replset)}" if self.replset else ""

        # Auth DB should be specified while user connects to application DB.
        auth_source = ""
        if self.database != "admin":
            # "&"" is needed to concatenate multiple values in URI
            auth_source = f"&{ADMIN_AUTH_SOURCE}" if self.replset else ADMIN_AUTH_SOURCE

        return (
            f"mongodb://{quote_plus(self.username)}:"
            f"{quote_plus(self.password)}@"
            f"{complete_hosts}/{quote_plus(self.database)}?"
            f"{replset_str}"
            f"{auth_source}"
        )


def supported_roles(config: MongoConfiguration):
    """Return the supported roles for the given configuration."""
    return REGULAR_ROLES | {"default": [{"db": config.database, "role": "readWrite"}]}


class MongoConnection:
    """In this class we create connection object to Mongo[s/db].

    This class is meant for agnositc functions in mongos and mongodb.

    Real connection is created on the first call to Mongo[s/db].
    Delayed connectivity allows to firstly check database readiness
    and reuse the same connection for an actual query later in the code.

    Connection is automatically closed when object destroyed.
    Automatic close allows to have more clean code.

    Note that connection when used may lead to the following pymongo errors: ConfigurationError,
    ConfigurationError, OperationFailure. It is suggested that the following pattern be adopted
    when using MongoDBConnection:

    with MongoMongos(MongoConfig) as mongo:
        try:
            mongo.<some operation from this class>
        except ConfigurationError, OperationFailure:
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
        self.config = config

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

    @property
    def is_ready(self) -> bool:
        """Is the MongoDB server ready for services requests.

        Returns:
            True if services is ready False otherwise. Retries over a period of 60 seconds times to
            allow server time to start up.

        """
        try:
            for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
                with attempt:
                    # The ping command is cheap and does not require auth.
                    self.client.admin.command("ping")
        except RetryError:
            return False

        return True

    def create_user(self, config: MongoConfiguration):
        """Create user.

        Grant read and write privileges for specified database.
        """
        self.client.admin.command(
            "createUser",
            config.username,
            pwd=config.password,
            roles=self._get_roles(config),
            mechanisms=["SCRAM-SHA-256"],
        )

    def update_user(self, config: MongoConfiguration):
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

    def create_role(self, role_name: str, privileges: dict, roles: dict = []):
        """Creates a new role.

        Args:
            role_name: name of the role to be added.
            privileges: privileges to be associated with the role.
            roles: List of roles from which this role inherits privileges.
        """
        try:
            self.client.admin.command(
                "createRole", role_name, privileges=[privileges], roles=roles
            )
        except OperationFailure as e:
            if not e.code == 51002:  # Role already exists
                logger.error("Cannot add role. error=%r", e)
                raise e

    @staticmethod
    def _get_roles(config: MongoConfiguration) -> List[dict]:
        all_roles = supported_roles(config)
        return list(chain.from_iterable(all_roles[role] for role in config.roles))

    def drop_user(self, username: str):
        """Drop user."""
        self.client.admin.command("dropUser", username)

    def get_users(self) -> Set[str]:
        """Add a new member to replica set config inside MongoDB."""
        users_info = self.client.admin.command("usersInfo")
        return set(
            [
                user_obj["user"]
                for user_obj in users_info["users"]
                if re.match(r"^relation-\d+$", user_obj["user"])
            ]
        )

    def get_databases(self) -> Set[str]:
        """Return list of all non-default databases."""
        databases = self.client.list_database_names()
        return {db for db in databases if db not in SYSTEM_DBS}

    def drop_database(self, database: str):
        """Drop a non-default database."""
        if database in SYSTEM_DBS:
            return
        self.client.drop_database(database)
