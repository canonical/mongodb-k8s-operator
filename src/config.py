"""Configuration for MongoDB Charm."""

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from typing import List, Literal

from ops.model import BlockedStatus


class Config:
    """Configuration for MongoDB Charm."""

    SUBSTRATE = "k8s"
    MONGOS_PORT = 27018
    MONGODB_PORT = 27017
    UNIX_USER = "mongodb"
    UNIX_GROUP = "mongodb"
    DATA_DIR = "/var/lib/mongodb"

    LOG_DIR = "/var/log/mongodb"
    MONGOD_CONF_DIR = "/etc/mongod"
    MONGODB_LOG_FILENAME = "mongodb.log"

    CHARM_INTERNAL_VERSION_FILE = "charm_internal_version"
    LICENSE_PATH = "/licenses/LICENSE"
    CONTAINER_NAME = "mongod"
    SERVICE_NAME = "mongod"
    SOCKET_PATH = "/tmp/mongodb-27017.sock"
    APP_SCOPE = "app"
    UNIT_SCOPE = "unit"

    # Keep these alphabetically sorted
    class Actions:
        """Actions related config for MongoDB Charm."""

        PASSWORD_PARAM_NAME = "password"
        USERNAME_PARAM_NAME = "username"

    class AuditLog:
        """Audit log related configuration."""

        FORMAT = "JSON"
        DESTINATION = "file"
        FILE_NAME = "audit.log"

    class Backup:
        """Backup related config for MongoDB Charm."""

        SERVICE_NAME = "pbm-agent"
        URI_PARAM_NAME = "pbm-uri"
        PBM_PATH = "/usr/bin/pbm"
        PBM_CONFIG_FILE_PATH = "/etc/pbm_config.yaml"

    class LogRotate:
        """Log rotate related constants."""

        MAX_LOG_SIZE = "50M"
        MAX_ROTATIONS_TO_KEEP = 10
        LOG_ROTATE_TEMPLATE = "./templates/logrotate.j2"
        RENDERED_TEMPLATE = "/etc/logrotate.d/mongodb"
        LOG_STATUS_DIR = "/var/lib/logrotate"

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
        JOBS = [{"static_configs": [{"targets": [f"*:{MONGODB_EXPORTER_PORT}"], "labels": []}]}]
        APP_SCOPE = "app"
        UNIT_SCOPE = "unit"

    class Relations:
        """Relations related config for MongoDB Charm."""

        APP_SCOPE = "app"
        UNIT_SCOPE = "unit"
        NAME = "database"
        PEERS = "database-peers"
        LOGGING = "logging"

        SHARDING_RELATIONS_NAME = "sharding"
        CONFIG_SERVER_RELATIONS_NAME = "config-server"
        CLUSTER_RELATIONS_NAME = "cluster"
        DB_RELATIONS = [NAME]
        Scopes = Literal[APP_SCOPE, UNIT_SCOPE]

    class Role:
        """Role config names for MongoDB Charm."""

        CONFIG_SERVER = "config-server"
        REPLICATION = "replication"
        SHARD = "shard"
        MONGOS = "mongos"

    class TLS:
        """TLS related config for MongoDB Charm."""

        EXT_PEM_FILE = "external-cert.pem"
        EXT_CA_FILE = "external-ca.crt"
        INT_PEM_FILE = "internal-cert.pem"
        INT_CA_FILE = "internal-ca.crt"
        KEY_FILE_NAME = "keyFile"
        TLS_PEER_RELATION = "certificates"

        SECRET_CA_LABEL = "ca-secret"
        SECRET_KEY_LABEL = "key-secret"
        SECRET_CERT_LABEL = "cert-secret"
        SECRET_CSR_LABEL = "csr-secret"
        SECRET_CHAIN_LABEL = "chain-secret"

    class Upgrade:
        """Upgrade related constants."""

        FEATURE_VERSION_6 = "6.0"

    class Secrets:
        """Secrets related constants."""

        SECRET_LABEL = "secret"
        SECRET_CACHE_LABEL = "cache"
        SECRET_KEYFILE_NAME = "keyfile"
        SECRET_INTERNAL_LABEL = "internal-secret"
        SECRET_DELETED_LABEL = "None"
        SECRET_KEYFILE_NAME = "keyfile"
        MAX_PASSWORD_LENGTH = 4096

    class Substrate:
        """Substrate related constants."""

        VM = "vm"
        K8S = "k8s"

    class Status:
        """Status related constants.

        TODO: move all status messages here.
        """

        STATUS_READY_FOR_UPGRADE = "status-shows-ready-for-upgrade"

        # TODO Future PR add more status messages here as constants
        UNHEALTHY_UPGRADE = BlockedStatus("Unhealthy after upgrade.")
        INCOMPATIBLE_UPGRADE = BlockedStatus(
            "Refresh incompatible. Rollback to previous revision with `juju refresh`"
        )

    @staticmethod
    def get_license_path(license_name: str) -> str:
        """Return the path to the license file."""
        return f"{Config.LICENSE_PATH}-{license_name}"

    @staticmethod
    def get_logs_files_paths() -> List[str]:
        """Returns list of paths to mongodb related log files."""
        return [
            f"{Config.LOG_DIR}/{Config.MONGODB_LOG_FILENAME}",
            f"{Config.LOG_DIR}/{Config.AuditLog.FILE_NAME}",
        ]
