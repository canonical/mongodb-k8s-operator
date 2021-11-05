import secrets
import string

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ServerSelectionTimeoutError
import logging


logger = logging.getLogger(__name__)

# We expect the mongodb container to use the
# default ports
MONGODB_PORT = 27017


class MongoDB():

    def __init__(self, config):
        """A MongoDB server interface.

        Args:
            config: a dict with the following keys
                - "app_name" : name of application
                - "replica_set_name" : name of replica set
                - "num_peers" : number of peer units
                - "port" : port on which MongoDB services is exposed
                - "root_password" : password for MongoDB root account
        """
        self.app_name = config['app_name']
        self.replica_set_name = config['replica_set_name']
        self.num_peers = config['num_peers']
        self.port = config['port']
        self.root_password = config['root_password']

    def client(self):
        """Construct a client for the MongoDB database.

        The timeout for all queries using this client object is 1 sec.

        Retruns:
            A pymongo :class:`MongoClient` object.
        """
        return MongoClient(self.replica_set_uri(), serverSelectionTimeoutMS=1000)

    def is_ready(self):
        """Is the MongoDB server ready to services requests.

        Returns:
            True if services is ready False otherwise.
        """
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
        """Reconfigure the MongoDB replica set.

        Args:
            hosts: a list of peer host addresses
        """
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
        """Initialize the MongoDB replica set.

        Args:
            hosts: a list of peer host addresses
        """
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

    def shutdown(self):
        """Shutdown MongoDB server.
        """
        if not self.is_ready():
            return

        client = self.client()
        db = client["admin"]
        try:
            db.command("shutdown")
        except (AutoReconnect, ServerSelectionTimeoutError):
            logger.debug("MongoDB shutdown failed")
        client.close()

    def new_user(self, credentials):
        """Create a new MongoDB user.

        Args:
            credentials: a dictionary with keys
                - "username": name of new user
                - "password": password of new user
        """
        client = self.client()
        db = client["admin"]
        db.command("createUser", credentials["username"],
                   pwd=credentials["password"],
                   roles=[])
        client.close()

    def drop_user(self, username):
        """Remove a MongoDB user.

        Args:
            username: string name of user
        """
        client = self.client()
        db = client["admin"]
        db.command("dropUser",
                   username)
        client.close()

    def new_databases(self, credentials, databases):
        """Create a new database in MongoDB.

        Args:
            credentials: a dictionary with key "username"
            databases: a list of new database names
        """
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
        """Remove a list of databases from MongoDB

        Args:
            databases: a list of database names
        """
        client = self.client()
        for database in databases:
            db = client[database]
            db.command("dropDatabase")
        client.close()

    def replica_set_uri(self, credentials=None):
        """Construct a replica set URI.

        Args:
            credentials: an optional dictionary with keys "username"
            and "password"

        Returns:
            A string URI that may be used to access the MongoDB
            replica set.
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
        """Construct a DNS name for a MongoDB unit.

        Args:
            id: an integer identifying the unit.

        Returns:
            A string representing the hostname of the MongoDB unit.
        """
        return "{0}-{1}.{0}-endpoints".format(self.app_name, id)

    @property
    def cluster_hosts(self) -> list:
        """Find all hostnames for MongoDB units.

        Returns:
            A list of hostnames of all MongoDB peer units.
        """
        return [self.hostname(i) for i in range(self.num_peers)]

    @property
    def databases(self):
        """List all databases currently available.

        Returns:
            A list of non default database names.
        """
        if not self.is_ready():
            return []

        client = self.client()
        # gather list of non default databases
        defaultdbs = ("admin", "local", "config")
        databases = client.list_database_names()
        databases = [db for db in databases if db not in defaultdbs]
        client.close()

        return databases

    @property
    def version(self):
        """Get MongoDB version.

        Returns:
            A string representing the MongoDB version.
        """
        client = self.client()
        try:
            info = client.server_info()
            return info['version']
        except ServerSelectionTimeoutError:
            logger.debug("mongodb service is not ready yet.")
        finally:
            client.close()
        return None

    @staticmethod
    def new_password():
        """Generate a random password string.

        Returns:
           A random password string.
        """
        choices = string.ascii_letters + string.digits
        pwd = "".join([secrets.choice(choices) for i in range(16)])
        return pwd
