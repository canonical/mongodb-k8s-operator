"""Simple functions, which can be used in both K8s and VM charms."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import os
import re
import secrets
import string
import subprocess
from typing import List, Optional, Union

from charms.mongodb.v0.mongodb import MongoDBConfiguration, MongoDBConnection
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    StatusBase,
    WaitingStatus,
)
from pymongo.errors import AutoReconnect, ServerSelectionTimeoutError

# The unique Charmhub library identifier, never change it
LIBID = "b9a7fe0c38d8486a9d1ce94c27d4758e"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 7


# path to store mongodb ketFile
KEY_FILE = "keyFile"
TLS_EXT_PEM_FILE = "external-cert.pem"
TLS_EXT_CA_FILE = "external-ca.crt"
TLS_INT_PEM_FILE = "internal-cert.pem"
TLS_INT_CA_FILE = "internal-ca.crt"

MONGODB_COMMON_DIR = "/var/snap/charmed-mongodb/common"
MONGODB_SNAP_DATA_DIR = "/var/snap/charmed-mongodb/current"


DATA_DIR = "/var/lib/mongodb"
CONF_DIR = "/etc/mongod"
MONGODB_LOG_FILENAME = "mongodb.log"
logger = logging.getLogger(__name__)


# noinspection GrazieInspection
def get_create_user_cmd(
    config: MongoDBConfiguration, mongo_path="charmed-mongodb.mongo"
) -> List[str]:
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


def get_mongod_args(
    config: MongoDBConfiguration,
    auth: bool = True,
    snap_install: bool = False,
) -> str:
    """Construct the MongoDB startup command line.

    Returns:
        A string representing the command used to start MongoDB.
    """
    full_data_dir = f"{MONGODB_COMMON_DIR}{DATA_DIR}" if snap_install else DATA_DIR
    full_conf_dir = f"{MONGODB_SNAP_DATA_DIR}{CONF_DIR}" if snap_install else CONF_DIR
    # in k8s the default logging options that are used for the vm charm are ignored and logs are
    # the output of the container. To enable logging to a file it must be set explicitly
    logging_options = "" if snap_install else f"--logpath={full_data_dir}/{MONGODB_LOG_FILENAME}"
    cmd = [
        # bind to localhost and external interfaces
        "--bind_ip_all",
        # part of replicaset
        f"--replSet={config.replset}",
        # db must be located within the snap common directory since the snap is strictly confined
        f"--dbpath={full_data_dir}",
        logging_options,
    ]
    if auth:
        cmd.extend(["--auth"])

    if auth and not config.tls_internal:
        # keyFile cannot be used without auth and cannot be used in tandem with internal TLS
        cmd.extend(
            [
                "--clusterAuthMode=keyFile",
                f"--keyFile={full_conf_dir}/{KEY_FILE}",
            ]
        )

    if config.tls_external:
        cmd.extend(
            [
                f"--tlsCAFile={full_conf_dir}/{TLS_EXT_CA_FILE}",
                f"--tlsCertificateKeyFile={full_conf_dir}/{TLS_EXT_PEM_FILE}",
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
                f"--tlsClusterCAFile={full_conf_dir}/{TLS_INT_CA_FILE}",
                f"--tlsClusterFile={full_conf_dir}/{TLS_INT_PEM_FILE}",
            ]
        )

    cmd.append("\n")
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
                return ActiveStatus("Primary")
            elif replica_status == "SECONDARY":
                return ActiveStatus("")
            elif replica_status in ["STARTUP", "STARTUP2", "ROLLBACK", "RECOVERING"]:
                return WaitingStatus("Member is syncing...")
            elif replica_status == "REMOVED":
                return WaitingStatus("Member is removing...")
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


def copy_licenses_to_unit():
    """Copies licenses packaged in the snap to the charm's licenses directory."""
    os.makedirs("src/licenses", exist_ok=True)
    subprocess.check_output("cp LICENSE src/licenses/LICENSE-charm", shell=True)
    subprocess.check_output(
        "cp -r /snap/charmed-mongodb/current/licenses/* src/licenses", shell=True
    )


_StrOrBytes = Union[str, bytes]


def process_pbm_error(error_string: Optional[_StrOrBytes]) -> str:
    """Parses pbm error string and returns a user friendly message."""
    message = "couldn't configure s3 backup option"
    if not error_string:
        return message
    if type(error_string) == bytes:
        error_string = error_string.decode("utf-8")
    if "status code: 403" in error_string:  # type: ignore
        message = "s3 credentials are incorrect."
    elif "status code: 404" in error_string:  # type: ignore
        message = "s3 configurations are incompatible."
    elif "status code: 301" in error_string:  # type: ignore
        message = "s3 configurations are incompatible."
    return message


def current_pbm_op(pbm_status: str) -> str:
    """Parses pbm status for the operation that pbm is running."""
    pbm_status_lines = pbm_status.splitlines()
    for i in range(0, len(pbm_status_lines)):
        line = pbm_status_lines[i]

        # operation is two lines after the line "Currently running:"
        if line == "Currently running:":
            return pbm_status_lines[i + 2]

    return ""


def process_pbm_status(pbm_status: str) -> StatusBase:
    """Parses current pbm operation and returns unit status."""
    if type(pbm_status) == bytes:
        pbm_status = pbm_status.decode("utf-8")

    # pbm is running resync operation
    if "Resync" in current_pbm_op(pbm_status):
        return WaitingStatus("waiting to sync s3 configurations.")

    # no operations are currently running with pbm
    if "(none)" in current_pbm_op(pbm_status):
        return ActiveStatus("")

    # Example of backup id: 2023-08-21T13:08:22Z
    backup_match = re.search(
        r'Snapshot backup "(?P<backup_id>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)"', pbm_status
    )
    if backup_match:
        backup_id = backup_match.group("backup_id")
        return MaintenanceStatus(f"backup started/running, backup id:'{backup_id}'")

    restore_match = re.search(
        r'Snapshot restore "(?P<backup_id>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)"', pbm_status
    )
    if restore_match:
        backup_id = restore_match.group("backup_id")
        return MaintenanceStatus(f"restore started/running, backup id:'{backup_id}'")

    return ActiveStatus()
