"""Configuration for MongoDB Charm."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


class Config:
    """Configuration for MongoDB Charm."""

    SUBSTRATE = "k8s"
    MONGODB_PORT = 27017
    UNIX_USER = "mongodb"
    UNIX_GROUP = "mongodb"
    DATA_DIR = "/var/lib/mongodb"
    CONF_DIR = "/etc/mongod"
    MONGODB_LOG_FILENAME = "mongodb.log"
    LOG_FILES = [f"{DATA_DIR}/{MONGODB_LOG_FILENAME}"]
    LICENSE_PATH = "/licenses/LICENSE"
    CONTAINER_NAME = "mongod"
    SERVICE_NAME = "mongod"
    SOCKET_PATH = "/tmp/mongodb-27017.sock"

    class Actions:
        """Actions related config for MongoDB Charm."""

        PASSWORD_PARAM_NAME = "password"
        USERNAME_PARAM_NAME = "username"

    class Backup:
        """Backup related config for MongoDB Charm."""

        SERVICE_NAME = "pbm-agent"
        URI_PARAM_NAME = "pbm-uri"

    class Monitoring:
        """Monitoring related config for MongoDB Charm."""

        MONGODB_EXPORTER_PORT = 9216
        METRICS_ENDPOINTS = [
            {"path": "/metrics", "port": f"{MONGODB_EXPORTER_PORT}"},
        ]
        METRICS_RULES_DIR = "./src/alert_rules/prometheus"
        LOGS_RULES_DIR = "./src/alert_rules/loki"
        LOG_SLOTS = ["charmed-mongodb:logs"]
        URI_PARAM_NAME = "monitor-uri"
        SERVICE_NAME = "mongodb-exporter"
        JOBS = [{"static_configs": [{"targets": [f"*:{MONGODB_EXPORTER_PORT}"]}]}]

    class Relations:
        """Relations related config for MongoDB Charm."""

        NAME = "database"
        PEERS = "database-peers"
        LOGGING = "logging"

    class TLS:
        """TLS related config for MongoDB Charm."""

        EXT_PEM_FILE = "external-cert.pem"
        EXT_CA_FILE = "external-ca.crt"
        INT_PEM_FILE = "internal-cert.pem"
        INT_CA_FILE = "internal-ca.crt"
        KEY_FILE_NAME = "keyFile"

    @staticmethod
    def get_license_path(license_name: str) -> str:
        """Return the path to the license file."""
        return f"{Config.LICENSE_PATH}-{license_name}"
