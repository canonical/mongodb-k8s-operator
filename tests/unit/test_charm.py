# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
from ops.model import ActiveStatus
from ops.testing import Harness

from charm import MongoDBCharm


@pytest.fixture
def harness():
    harness = Harness(MongoDBCharm)
    mongo_resource = {
        "registrypath": "mongo:4.4",
    }
    harness.add_oci_resource("mongodb-image", mongo_resource)
    harness.begin()
    harness.add_relation("database-peers", "mongodb-peers")
    harness.set_leader(True)
    yield harness
    harness.cleanup()


def test_mongod_pebble_ready(harness):
    # Expected plan after Pebble ready with default config
    expected_plan = {
        "services": {
            "mongod": {
                "user": "mongodb",
                "group": "mongodb",
                "override": "replace",
                "summary": "mongod",
                "command": (
                    "mongod --bind_ip_all --auth "
                    "--replSet=mongodb-k8s "
                    "--clusterAuthMode=keyFile "
                    "--keyFile=/etc/mongodb/keyFile"
                ),
                "startup": "enabled",
            }
        },
    }
    # Get the mongod container from the model
    container = harness.model.unit.get_container("mongod")
    harness.set_can_connect(container, True)
    # Emit the PebbleReadyEvent carrying the mongod container
    harness.charm.on.mongod_pebble_ready.emit(container)
    # Get the plan now we've run PebbleReady
    updated_plan = harness.get_container_pebble_plan("mongod").to_dict()
    # Check we've got the plan we expected
    assert expected_plan == updated_plan
    # Check the service was started
    service = harness.model.unit.get_container("mongod").get_service("mongod")
    assert service.is_running()
    # Ensure we set an ActiveStatus with no message
    assert harness.model.unit.status == ActiveStatus()
