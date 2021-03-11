#!/usr/bin/env python3
import logging

SECRET_PATH = "/var/lib/mongodb-secrets"
KEY_FILE = "key.file"

logger = logging.getLogger(__name__)


class PodSpecBuilder:
    def __init__(self, config):
        self.name = config.get("name")
        self.replica_set_name = config.get("replica_set_name")
        self.port = config.get("port", 27017)
        self.image_info = config.get("image_info", {})
        self.root_password = config.get("root_password")
        self.security_key = config.get("security_key")

    def _make_pod_command(self):
        command = ["mongod"]
        bind_ip_option = "--bind_ip 0.0.0.0"
        replica_set_option = (
            "--replSet {}".format(self.replica_set_name) if self.replica_set_name else None
        )
        keyfile_option = "--keyFile {}/{}".format(SECRET_PATH, KEY_FILE)

        command.extend(bind_ip_option.split(" "))
        if replica_set_option:
            command.extend(replica_set_option.split(" "))
        command.extend(keyfile_option.split(" "))

        return command

    def _make_pod_ports(self):
        return [{
            "name": "mongodb",
            "containerPort": self.port,
            "protocol": "TCP"}]

    def _make_readiness_probe(self):
        return {
            "tcpSocket": {"port": self.port},
            "timeoutSeconds": 5,
            "periodSeconds": 5,
            "initialDelaySeconds": 10,
        }

    def _make_liveness_probe(self):
        return {
            "exec": {"command": ["pgrep", "mongod"]},
            "initialDelaySeconds": 45,
            "timeoutSeconds": 5,
        }

    def _make_service_account(self):
        return {
            "roles": [
                {
                    "rules": [
                        {
                            "apiGroups": [""],
                            "resources": ["pods"],
                            "verbs": ["list"],
                        }
                    ]
                }
            ]
        }

    def _make_env_config(self):
        env = {
            "MONGO_INITDB_ROOT_USERNAME": "root",
            "MONGO_INITDB_ROOT_PASSWORD": self.root_password,
            "MONGO_INITDB_DATABASE": "admin"
        }

        return env

    def _make_volume_config(self):
        volume_config = []

        key_file_config = {
            "name": "secrets",
            "mountPath": SECRET_PATH,
            "emptyDir": {}
        }

        volume_config.append(key_file_config)
        return volume_config

    def make_pod_spec(self):
        command = self._make_pod_command()
        ports = self._make_pod_ports()
        readiness_probe = self._make_readiness_probe()
        liveness_probe = self._make_liveness_probe()
        service_account = self._make_service_account()
        volume_config = self._make_volume_config()
        env = self._make_env_config()

        return {
            "version": 3,
            "serviceAccount": service_account,
            "containers": [
                {
                    "name": self.name,
                    "imageDetails": self.image_info,
                    "imagePullPolicy": "Always",
                    "args": command,
                    "ports": ports,
                    "envConfig": env,
                    "kubernetes": {
                        "readinessProbe": readiness_probe,
                        "livenessProbe": liveness_probe,
                    },
                    "volumeConfig": volume_config
                },
                {
                    "name": "mongodb-init",
                    "imageDetails": self.image_info,
                    "imagePullPolicy": "Always",
                    "envConfig": {
                        "SECRET": self.security_key
                    },
                    "init": True,
                    "volumeConfig": volume_config,
                    "command": [
                        "sh",
                        "-c"
                    ],
                    "args": [
                        "echo ${{SECRET}} >> {0}/{1} && "
                        "chmod 0400 {0}/{1} && "
                        "chown mongodb.mongodb {0}/{1}".format(
                            SECRET_PATH,
                            KEY_FILE
                        )
                    ]
                }
            ],
            "kubernetesResources": {},
        }
