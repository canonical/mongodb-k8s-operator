#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
"""Charm code for MongoDB service on Kubernetes."""

import logging

from charmlibs.interfaces.service_mesh import ServiceMeshConsumer, UnitPolicy
from ops.log import JujuLogHandler
from ops.main import main
from single_kernel_mongo.abstract_charm import AbstractMongoCharm
from single_kernel_mongo.config.literals import Substrates
from single_kernel_mongo.config.relations import PeerRelationNames
from single_kernel_mongo.core.structured_config import MongoDBCharmConfig
from single_kernel_mongo.managers.mongodb_operator import MongoDBOperator

# Show logger name (module name) in logs
root_logger = logging.getLogger()
for handler in root_logger.handlers:
    if isinstance(handler, JujuLogHandler):
        handler.setFormatter(logging.Formatter("{name}:{message}", style="{"))
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class MongoDBK8sCharm(AbstractMongoCharm[MongoDBCharmConfig, MongoDBOperator]):
    """Charm the service."""

    config_type = MongoDBCharmConfig
    operator_type = MongoDBOperator
    substrate = Substrates.K8S
    peer_rel_name = PeerRelationNames.PEERS
    name = "mongodb-k8s"

    def __init__(self, *args):
        """Initialize the charm."""
        super().__init__(*args)

        # Service mesh integration
        self.mesh = ServiceMeshConsumer(
            self,
            policies=[
                # Prometheus metrics scraping
                UnitPolicy(
                    relation="self-metrics-endpoint",
                    ports=[9216],
                ),
                # Client database connections
                UnitPolicy(
                    relation="database",
                    ports=[27017],
                ),
                # Replica set peer communication
                UnitPolicy(
                    relation="database-peers",
                    ports=[27017],
                ),
                # Config server for sharded clusters
                UnitPolicy(
                    relation="config-server",
                    ports=[27017, 27018],
                ),
                # Mongos cluster access
                UnitPolicy(
                    relation="cluster",
                    ports=[27017, 27018],
                ),
                # Vault integration for encryption at rest
                UnitPolicy(
                    relation="vault-kv",
                    ports=[8200],
                ),
            ],
        )


if __name__ == "__main__":
    main(MongoDBK8sCharm)
