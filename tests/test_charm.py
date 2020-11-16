# Copyright 2020 Canonical Ltd
# See LICENSE file for licensing details.

import unittest

from ops.testing import Harness
from charm import MongoDBCharm


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(MongoDBCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_replica_set_name_can_be_changed(self):
        pass
