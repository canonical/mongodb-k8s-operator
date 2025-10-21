#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
"""Charm code for MongoDB service on Kubernetes."""

import logging

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


if __name__ == "__main__":
    main(MongoDBK8sCharm)
