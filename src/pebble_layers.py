#!/usr/bin/env python3
import logging
from ops.pebble import Layer

KEY_FILE = "/key.file"

logger = logging.getLogger(__name__)


class MongoLayers:
    def __init__(self, config):
        """Pebble layers for MongoDB.

        Args:
            config: a dict with the following keys
                - replica_set_name : name of replica set
                - port : port on which monogodb service should be exposed
                - root_password : password for MongoDB root account
        """
        self._replica_set_name = config.get("replica_set_name")
        self._port = config.get("port", 27017)
        self._root_password = config.get("root_password")

    def build(self):
        """Generate the MongoDB layer for Pebble.

        Returns:
            A dictionary representing the MongoDB pebble layer.
        """
        layer_spec = {
            "summary": "MongoDB layer",
            "description": "Pebble layer configuration for replicated MongoDB",
            "services": {
                "mongodb": {
                    "override": "replace",
                    "summary": "mongod daemon",
                    "command": self._command(),
                    "startup": "enabled",
                    "environment": {
                        "MONGO_INITDB_ROOT_USERNAME": "root",
                        "MONGO_INITDB_ROOT_PASSWORD": self._root_password,
                        "MONGO_INITDB_DATABASE": "admin"
                    }
                }
            },
        }
        return Layer(layer_spec)

    def _command(self):
        """Construct the MongoDB startup command line.

        Returns:
            A string representing the command used to start MongoDB in the
            workload container.
        """
        cmd = ["/usr/local/bin/docker-entrypoint.sh"]

        cmd.extend(self._command_arguments())
        return " ".join(cmd)

    def _command_arguments(self):
        """Construct the list of MongoDB command line arguments.

        Returns:
            A list containing MongoDB command line arguments.
        """
        args = ["mongod"]
        replica_set_option = "--replSet {}".format(self._replica_set_name)
        keyfile_option = "--keyFile {}".format(KEY_FILE)
        args.extend(replica_set_option.split(" "))
        args.extend(keyfile_option.split(" "))
        return args
