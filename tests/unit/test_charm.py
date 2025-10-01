# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import unittest
from unittest.mock import patch

import pytest
from ops.testing import Harness

from charm import MongoDBK8sCharm

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def patch_k8s_manager(monkeypatch):
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.get_partition",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        "single_kernel_mongo.managers.k8s.K8sManager.get_pod",
        lambda *args, **kwargs: 0,
    )


class TestCharm(unittest.TestCase):
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    def setUp(self, *unused):
        self.maxDiff = None
        self.harness = Harness(MongoDBK8sCharm)
        mongo_resource = {
            "registrypath": "mongo:4.4",
        }
        self.harness.add_oci_resource("mongodb-image", mongo_resource)
        self.harness.add_relation("database-peers", "mongodb-peers")
        self.harness.add_relation("upgrade-version-a", "upgrade-version-a")
        self.harness.begin()
        with self.harness.hooks_disabled():
            self.harness.add_storage(storage_name="mongodb", count=1, attach=True)
            self.harness.add_storage(storage_name="mongodb-logs", count=1, attach=True)
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator._initialise_replica_set")
    @patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.exec")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.handle_licenses")
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.set_permissions")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_mongod_pebble_ready(
        self, connect_exporter, fix_data_dir, defer, pull_licenses, exec, init_repl
    ):
        # Expected plan after Pebble ready with default config
        expected_plan = {
            "services": {
                "mongod": {
                    "user": "mongodb",
                    "group": "mongodb",
                    "override": "replace",
                    "summary": "mongod",
                    "command": "/bin/bash /bin/start-mongod.sh",
                    "environment": {"MONGOD_ARGS": ""},
                    "startup": "enabled",
                },
            },
        }
        # Get the mongod container from the model
        container = self.harness.model.unit.get_container("mongod")
        self.harness.set_can_connect(container, True)
        container.make_dir("/etc/logrotate.d", make_parents=True)
        # Emit the PebbleReadyEvent carrying the mongod container
        self.harness.charm.on.mongod_pebble_ready.emit(container)
        # Get the plan now we've run PebbleReady
        updated_plan = self.harness.get_container_pebble_plan("mongod").to_dict()
        # Check we've got the plan we expected
        assert expected_plan == updated_plan
        # Check the service was started
        service = self.harness.model.unit.get_container("mongod").get_service("mongod")
        assert service.is_running()
        defer.assert_not_called()
        # Ensure that _connect_mongodb_exporter was called
        connect_exporter.assert_called_once()
