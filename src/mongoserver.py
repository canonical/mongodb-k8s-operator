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

    def get_client(self):
        return MongoClient(self.standalone_uri, serverSelectionTimeoutMS=1000)

    def get_replica_set_client(self):
        return MongoClient(self.replica_set_uri, serverSelectionTimeoutMS=1000)

    def is_ready(self):
        ready = False
        client = self.get_client()
        try:
            client.server_info()
            ready = True
            logger.debug("mongodb service is ready.")
        except ServerSelectionTimeoutError:
            logger.debug("mongodb service is not ready yet.")
        finally:
            client.close()
        return ready

    def reconfigure_replica_set(self, hosts: list):
        replica_set_client = self.get_replica_set_client()
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
        client = self.get_client()
        try:
            logger.debug("initializing replica set with config={}".format(config))
            client.admin.command("replSetInitiate", config)
        except Exception as e:
            logger.error("cannot initialize replica set. error={}".format(e))
            raise e
        finally:
            client.close()

    @property
    def replica_set_uri(self):
        """Construct a replica set URI
        """
        uri = "mongodb://"
        for i, host in enumerate(self.cluster_hosts):
            if i:
                uri += ","
            uri += "{}:{}".format(host, self.port)
        uri += "/"
        return uri

    @property
    def standalone_uri(self):
        """Construct a standalone URI
        """
        return "mongodb://{}:{}/".format(self.app_name,
                                         self.port)

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
    def version(self):
        """Get MongoDB version
        """
        client = self.get_client()
        info = client.server_info()
        return info['version']
