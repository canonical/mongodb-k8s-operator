"""Simple functions, which can be used in both K8s and VM charms."""
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import secrets
import string
from typing import List

from charms.mongodb.v0.mongodb import MongoDBConfiguration, MongoDBConnection
from ops.model import ActiveStatus, BlockedStatus, StatusBase, WaitingStatus
from pymongo.errors import AutoReconnect, ServerSelectionTimeoutError

# The unique Charmhub library identifier, never change it
LIBID = "b9a7fe0c38d8486a9d1ce94c27d4758e"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 3


# path to store mongodb ketFile
KEY_FILE = "/etc/mongodb/keyFile"
TLS_EXT_PEM_FILE = "/etc/mongodb/external-cert.pem"
TLS_EXT_CA_FILE = "/etc/mongodb/external-ca.crt"
TLS_INT_PEM_FILE = "/etc/mongodb/internal-cert.pem"
TLS_INT_CA_FILE = "/etc/mongodb/internal-ca.crt"


logger = logging.getLogger(__name__)


# noinspection GrazieInspection
def get_create_user_cmd(config: MongoDBConfiguration, mongo_path="mongo") -> List[str]:
    """Creates initial admin user for MongoDB.

    Initial admin user can be created only through localhost connection.
    see https://www.mongodb.com/docs/manual/core/localhost-exception/
    unfortunately, pymongo not able to create connection which considered
    as local connection by MongoDB, even if socket connection used.
    As result where are only hackish ways to create initial user.
    It is needed to install mongodb-clients inside charm container to make
    this function work correctly
    """
    return [
        mongo_path,
        "mongodb://localhost/admin",
        "--quiet",
        "--eval",
        "db.createUser({"
        f"  user: '{config.username}',"
        "  pwd: passwordPrompt(),"
        "  roles:["
        "    {'role': 'userAdminAnyDatabase', 'db': 'admin'}, "
        "    {'role': 'readWriteAnyDatabase', 'db': 'admin'}, "
        "    {'role': 'clusterAdmin', 'db': 'admin'}, "
        "  ],"
        "  mechanisms: ['SCRAM-SHA-256'],"
        "  passwordDigestor: 'server',"
        "})",
    ]


def get_mongod_cmd(config: MongoDBConfiguration) -> str:
    """Construct the MongoDB startup command line.

    Returns:
        A string representing the command used to start MongoDB.
    """
    cmd = [
        "mongod",
        # bind to localhost and external interfaces
        "--bind_ip_all",
        # enable auth
        "--auth",
        # part of replicaset
        f"--replSet={config.replset}",
    ]
    if config.tls_external:
        cmd.extend(
            [
                f"--tlsCAFile={TLS_EXT_CA_FILE}",
                f"--tlsCertificateKeyFile={TLS_EXT_PEM_FILE}",
                # allow non-TLS connections
                "--tlsMode=preferTLS",
            ]
        )

    # internal TLS can be enabled only in external is enabled
    if config.tls_internal and config.tls_external:
        cmd.extend(
            [
                "--clusterAuthMode=x509",
                "--tlsAllowInvalidCertificates",
                f"--tlsClusterCAFile={TLS_INT_CA_FILE}",
                f"--tlsClusterFile={TLS_INT_PEM_FILE}",
            ]
        )
    else:
        # keyFile used for authentication replica set peers if no internal tls configured.
        cmd.extend(
            [
                "--clusterAuthMode=keyFile",
                f"--keyFile={KEY_FILE}",
            ]
        )
    return " ".join(cmd)


def generate_password() -> str:
    """Generate a random password string.

    Returns:
       A random password string.
    """
    choices = string.ascii_letters + string.digits
    return "".join([secrets.choice(choices) for _ in range(32)])


def generate_keyfile() -> str:
    """Key file used for authentication between replica set peers.

    Returns:
       A maximum allowed random string.
    """
    choices = string.ascii_letters + string.digits
    return "".join([secrets.choice(choices) for _ in range(1024)])


def build_unit_status(mongodb_config: MongoDBConfiguration, unit_ip: str) -> StatusBase:
    """Generates the status of a unit based on its status reported by mongod."""
    try:
        with MongoDBConnection(mongodb_config) as mongo:
            replset_status = mongo.get_replset_status()

            if unit_ip not in replset_status:
                return WaitingStatus("Member being added..")

            replica_status = replset_status[unit_ip]

            if replica_status == "PRIMARY":
                return ActiveStatus("Replica set primary")
            elif replica_status == "SECONDARY":
                return ActiveStatus("Replica set secondary")
            elif replica_status in ["STARTUP", "STARTUP2", "ROLLBACK", "RECOVERING"]:
                return WaitingStatus("Member is syncing..")
            elif replica_status == "REMOVED":
                return WaitingStatus("Member is removing..")
            else:
                return BlockedStatus(replica_status)
    except ServerSelectionTimeoutError as e:
        # ServerSelectionTimeoutError is commonly due to ReplicaSetNoPrimary
        logger.debug("Got error: %s, while checking replica set status", str(e))
        return WaitingStatus("Waiting for primary re-election..")
    except AutoReconnect as e:
        # AutoReconnect is raised when a connection to the database is lost and an attempt to
        # auto-reconnect will be made by pymongo.
        logger.debug("Got error: %s, while checking replica set status", str(e))
        return WaitingStatus("Waiting to reconnect to unit..")
