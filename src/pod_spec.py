#!/usr/bin/env python3
import logging

logger = logging.getLogger(__name__)


class PodSpecBuilder:
    def __init__(
        self, name: str, replica_set_name: str = None, port: int = 27017, image_info={}
    ):
        self.name = name
        self.replica_set_name = replica_set_name
        self.port = port
        self.image_info = image_info

    def _make_pod_command(self):
        command = ["mongod"]
        bind_ip_option = "--bind_ip 0.0.0.0"
        replica_set_option = (
            f"--replSet {self.replica_set_name}" if self.replica_set_name else None
        )

        command.extend(bind_ip_option.split(" "))
        if replica_set_option:
            command.extend(replica_set_option.split(" "))

        return command

    def _make_pod_ports(self):
        return [
            {"name": "mongodb", "containerPort": self.port, "protocol": "TCP"},
        ]

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

    def make_pod_spec(self):
        command = self._make_pod_command()
        ports = self._make_pod_ports()
        readiness_probe = self._make_readiness_probe()
        liveness_probe = self._make_liveness_probe()
        service_account = self._make_service_account()

        return {
            "version": 3,
            "serviceAccount": service_account,
            "containers": [
                {
                    "name": self.name,
                    "imageDetails": self.image_info,
                    "imagePullPolicy": "Always",
                    "command": command,
                    "ports": ports,
                    "kubernetes": {
                        "readinessProbe": readiness_probe,
                        "livenessProbe": liveness_probe,
                    },
                }
            ],
            "kubernetesResources": {},
        }
