#!/usr/bin/env python3
import logging

SECRET_PATH = "/var/lib/mongodb-secrets"
KEY_FILE = "key.file"

logger = logging.getLogger(__name__)


class MongoLayers:
    def __init__(self, config):
        self._replica_set_name = config.get("replica_set_name")
        self._port = config.get("port", 27017)
        self._root_password = config.get("root_password")

    def build(self):
        layer = {
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
        return layer

    def _command(self):
        cmd = ["/usr/local/bin/docker-entrypoint.sh"]

        cmd.extend(self._command_arguments())
        return " ".join(cmd)

    def _command_arguments(self):
        args = ["mongod"]
        replica_set_option = "--replSet {}".format(self._replica_set_name)
        keyfile_option = "--keyFile {}/{}".format(SECRET_PATH, KEY_FILE)
        args.extend(replica_set_option.split(" "))
        args.extend(keyfile_option.split(" "))
        return args
