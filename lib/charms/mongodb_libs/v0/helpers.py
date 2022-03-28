import string
import secrets
import logging
from typing import List
from functools import wraps
from charms.mongodb_libs.v0.mongodb import MongoDBConfiguration

# The unique Charmhub library identifier, never change it
LIBID = "1057f353503741a98ed79309b5be7e31"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 0

# path to store mongodb ketFile
KEY_FILE = "/tmp/keyFile"


logger = logging.getLogger(__name__)


# noinspection GrazieInspection
def get_create_user_cmd(config: MongoDBConfiguration) -> List[str]:
    """Creates initial admin user for MongoDB

    Initial admin user can be created only through localhost connection.
    see https://www.mongodb.com/docs/manual/core/localhost-exception/
    unfortunately, pymongo not able to create connection which considered
    as local connection by MongoDB, even if socket connection used.
    As result where are only hackish ways to create initial user.
    It is needed to install mongodb-clients inside charm container to make
    this function work correctly
    """
    return [
        "mongo",
        "mongodb://localhost/admin",
        "--quiet",
        "--eval",
        "db.createUser({"
        f"  user: '{config.admin_user}',"
        "  pwd: passwordPrompt(),"
        "  roles:["
        "    {'role': 'userAdmin', 'db': 'admin'}, "
        "    {'role': 'clusterAdmin', 'db': 'admin'}, "
        "  ],"
        "  mechanisms: ['SCRAM-SHA-256'],"
        "  passwordDigestor: 'server',"
        "})",
    ]


def get_mongod_cmd(config: MongoDBConfiguration) -> str:
    """Construct the MongoDB startup command line.

    Returns:
        A string representing the command used to start MongoDB in the
        workload container.
    """
    cmd = [
        "mongod",
        # bind to localhost and external interfaces
        "--bind_ip_all",
        # enable auth
        "--auth",
        # part of replicaset
        f"--replSet={config.replset_name}",
        # keyFile used for authentication replica set peers
        # TODO: replace with x509
        "--clusterAuthMode=keyFile",
        f"--keyFile={KEY_FILE}",
        # TODO: add TLS certificates paths
        # allow self signed certificates
        # cmd.append("--tlsAllowInvalidCertificates")
    ]
    return " ".join(cmd)


def generate_password() -> str:
    """Generate a random password string.

    Returns:
       A random password string.
    """
    choices = string.ascii_letters + string.digits
    return "".join([secrets.choice(choices) for _ in range(32)])


def generate_keyfile() -> str:
    """keyFile used for authentication between replica set peers.

    Returns:
       A maximum allowed random string.
    """
    choices = string.ascii_letters + string.digits
    return "".join([secrets.choice(choices) for _ in range(1024)])


def log_signal(f):
    @wraps(f)
    def _inner(*args, **kwargs):
        try:
            logger.debug(f"Running {f.__name__}")
            return f(*args, **kwargs)
        finally:
            logger.debug(f"Finished {f.__name__}")

    return _inner
