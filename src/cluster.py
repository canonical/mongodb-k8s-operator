import logging

from ops.charm import CharmEvents
from ops.framework import Object, StoredState, EventBase, EventSource

logger = logging.getLogger(__name__)


class MongoDBStartedEvent(EventBase):
    pass


class MongoDBReadyEvent(EventBase):
    pass


class ReplicaSetConfiguredEvent(EventBase):
    def __init__(self, handle, hosts):
        super().__init__(handle)
        self.hosts = hosts

    def snapshot(self):
        return {"hosts": self.hosts}

    def restore(self, snapshot):
        self.hosts = snapshot["hosts"]


class MongoDBClusterEvents(CharmEvents):
    mongodb_started = EventSource(MongoDBStartedEvent)
    cluster_ready = EventSource(MongoDBReadyEvent)
    replica_set_configured = EventSource(ReplicaSetConfiguredEvent)


class MongoDBCluster(Object):

    state = StoredState()
    on = MongoDBClusterEvents()

    def __init__(self, charm, relation_name, port):
        super().__init__(charm, relation_name)

        self.state.set_default(ready=None)
        self.state.set_default(replica_set_hosts=None)
        self.port = port

