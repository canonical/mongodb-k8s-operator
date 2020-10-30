import socket
import json
import logging

from ops.framework import Object, StoredState

logger = logging.getLogger(__name__)

"""
Relation data

App data:

{
    "ready": "True" or "False"
    "replica_set_hosts": "[mongodb1, mongodb2, mongodb3, ...]" 
}

ready: Indicates whether the cluster is ready or not,
replica_set_hosts: List of units of the replica set
"""


class MongoDBCluster(Object):

    state = StoredState()

    def __init__(self, charm, relation_name, port):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)

        self.framework.observe(charm.on.cluster_ready, self.on_cluster_ready)
        self.framework.observe(
            charm.on.replica_set_configured, self.on_replica_set_configured
        )

        self.state.set_default(ready=None)
        self.state.set_default(replica_set_hosts=None)
        self.port = port

    def on_cluster_ready(self, event):
        if not self.framework.model.unit.is_leader():
            return

        self.state.ready = True

        if not self.is_joined:
            logger.debug("on_cluster_ready: Not relation not joined yet")
            event.defer()
            return

        ready = str(self.state.ready)
        self._relation.data[self.model.app]["ready"] = ready

        logger.debug(f"Relation data updated: ready={ready}")

    def on_replica_set_configured(self, event):
        if not self.framework.model.unit.is_leader():
            return

        self.state.replica_set_hosts = event.hosts

        if not self.is_joined:
            logger.debug("on_replica_set_configured: Not relation not joined yet")
            event.defer()
            return
        replica_set_hosts = str(self.state.replica_set_hosts)
        self._relation.data[self.model.app]["replica_set_hosts"] = replica_set_hosts

        logger.debug(f"Relation data updated: replica_set_hosts={replica_set_hosts}")

    @property
    def is_joined(self):
        return self._relation is not None

    @property
    def replica_set_initialized(self):
        return self.replica_set_hosts is not None

    @property
    def replica_set_hosts(self):

        if not self.state.replica_set_hosts and self.is_joined:
            hosts = self._relation.data[self.model.app].get("replica_set_hosts")
            if hosts:
                self.state.replica_set_hosts = json.loads(hosts)
        return self.state.replica_set_hosts

    @property
    def num_units(self):
        return len(self._relation.units) + 1 if self.is_joined else 1

    def _get_unit_hostname(self, _id: int) -> str:
        return f"{self.model.app.name}-{_id}.{self.model.app.name}-endpoints"

    @property
    def hosts(self: int) -> list:
        return [self._get_unit_hostname(i) for i in range(self.num_units)]

    @property
    def ready(self):
        if not self.state.ready and self.is_joined:
            ready = self._relation.data[self.model.app].get("ready", False)
            self.state.started = bool(ready)
        return self.state.ready

    @property
    def replica_set_uri(self):
        uri = "mongodb://"
        for i, host in enumerate(self.hosts):
            if i:
                uri += ","
            uri += f"{host}:{self.port}"
        uri += "/"
        return uri

    @property
    def standalone_uri(self):
        return f"mongodb://{self.model.app.name}:{self.port}/"

    def need_replica_set_reconfiguration(self):
        return self.hosts != self.replica_set_hosts
