import secrets
import string

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import logging


logger = logging.getLogger(__name__)

# We expect the mongodb container to use the
# default ports
MONGODB_PORT = 27017


class MongoDB():

    def __init__(self, config):
        self.app_name = config['app_name']
        self.replica_set_name = config['replica_set_name']
        self.num_peers = config['num_peers']
        self.port = config['port']
        self.root_password = config['root_password']

    def client(self):
        return MongoClient(self.replica_set_uri(), serverSelectionTimeoutMS=1000)

    def is_ready(self):
        ready = False
        client = self.client()
        try:
            client.server_info()
            ready = True
        except ServerSelectionTimeoutError:
            logger.debug("mongodb service is not ready yet.")
        finally:
            client.close()
        return ready

    def reconfigure_replica_set(self, hosts: list):
        replica_set_client = self.client()
        try:
            rs_config = replica_set_client.admin.command("replSetGetConfig")
            rs_config["config"]["_id"] = self.replica_set_name
            rs_config["config"]["members"] = [
                {"_id": i, "host": h} for i, h in enumerate(hosts)
            ]
            rs_config["config"]["version"] += 1
            replica_set_client.admin.command(
                "replSetReconfig", rs_config["config"], force=True
            )
        except Exception as e:
            logger.error("cannot reconfigure replica set. error={}".format(e))
            raise e
        finally:
            replica_set_client.close()

    def initialize_replica_set(self, hosts: list):
        config = {
            "_id": self.replica_set_name,
            "members": [{"_id": i, "host": h} for i, h in enumerate(hosts)],
        }
        client = self.client()
        try:
            client.admin.command("replSetInitiate", config)
        except Exception as e:
            logger.error("cannot initialize replica set. error={}".format(e))
            raise e
        finally:
            client.close()

    def new_user(self, credentials):
        client = self.client()
        db = client["admin"]
        db.command("createUser", credentials["username"],
                   pwd=credentials["password"],
                   roles=[])
        client.close()

    def drop_user(self, username):
        client = self.client()
        db = client["admin"]
        db.command("dropUser",
                   username)
        client.close()

    def new_databases(self, credentials, databases):
        client = self.client()
        db = client["admin"]
        roles = []
        for database in databases:
            roles.append({"role": "readWrite",
                          "db": database})
        db.command("updateUser",
                   credentials["username"],
                   roles=roles)
        client.close()

    def drop_databases(self, databases):
        client = self.client()
        for database in databases:
            db = client[database]
            db.command("dropDatabase")
        client.close()

    def replica_set_uri(self, credentials=None):
        """Construct a replica set URI
        """
        if credentials:
            password = credentials["password"]
            username = credentials["username"]
        else:
            password = self.root_password
            username = "root"

        uri = "mongodb://{}:{}@".format(
            username,
            password)
        for i, host in enumerate(self.cluster_hosts):
            if i:
                uri += ","
            uri += "{}:{}".format(host, self.port)
        uri += "/admin"
        return uri

    def hostname(self, id: int) -> str:
        """Construct a DNS name for a MongoDB unit
        """
        return "{}-{}.{}-endpoints".format(self.app_name,
                                           id,
                                           self.app_name)

    @property
    def cluster_hosts(self) -> list:
        """Find all hostnames for MongoDB units
        """
        return [self.hostname(i) for i in range(self.num_peers)]

    @property
    def databases(self):
        """List all databases currently available
        """
        if not self.is_ready():
            return []

        client = self.client()
        # gather list of no default databases
        defaultdbs = ("admin", "local", "config")
        databases = client.list_database_names()
        databases = [db for db in databases if db not in defaultdbs]
        client.close()

        return databases

    @property
    def version(self):
        """Get MongoDB version
        """
        client = self.client()
        info = client.server_info()
        client.close()
        return info['version']

    @staticmethod
    def new_password():
        choices = string.ascii_letters + string.digits
        pwd = "".join([secrets.choice(choices) for i in range(16)])
        return pwd
