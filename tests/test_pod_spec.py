# Copyright 2021 Canonical Ltd
# See LICENSE file for licensing details.

import unittest

from ops.testing import Harness
from charm import MongoDBCharm
from pod_spec import PodSpecBuilder

POD_CONFIG = {
    "name": "test",
    "replica_set_name": "replset",
    "port": "12345",
    "image_info": {
        "name": "mongodb:latest"
    },
    "root_password": "pass",
    "security_key": "akey"
}


class TestPodSpecBuilder(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(MongoDBCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_pod_spec_has_two_containers(self):
        config = POD_CONFIG.copy()
        builder = PodSpecBuilder(config)
        pod_spec = builder.make_pod_spec()
        containers = get_container(pod_spec)
        self.assertEqual(len(containers), 2)

    def test_init_container_environment_contains_key(self):
        config = POD_CONFIG.copy()
        builder = PodSpecBuilder(config)
        pod_spec = builder.make_pod_spec()
        container = get_container(pod_spec, "mongodb-init")
        self.assertIn("SECRET", container["envConfig"])


def get_container(pod_spec, name=None):
    if name is None:
        # return all containers
        return pod_spec["containers"]
    else:
        for c in pod_spec["containers"]:
            if c["name"] == name:
                return c
