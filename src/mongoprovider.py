import json
import logging

from ops.framework import StoredState
from ops.relation import Provider
from mongoserver import MongoDB

logger = logging.getLogger(__name__)


class MongoProvider(Provider):
    stored = StoredState()

    def __init__(self, charm, name, provides):
        super().__init__(charm, name, provides)
        self.charm = charm
        self.stored.set_default(consumers={})
        events = self.charm.on[name]
        self.framework.observe(events.relation_joined,
                               self.on_database_relation_joined)
        self.framework.observe(events.relation_changed,
                               self.on_database_relation_changed)
        self.framework.observe(events.relation_broken,
                               self.on_database_relation_broken)

    ##############################################
    #               RELATIONS                    #
    ##############################################
    def on_database_relation_joined(self, event):
        if not self.charm.unit.is_leader():
            return

        rel_id = event.relation.id
        creds = self.credentials(rel_id)
        self.charm.mongo.new_user(creds)
        standalone_uri = "{}".format(self.charm.mongo.standalone_uri(creds))
        replica_set_uri = "{}".format(self.charm.mongo.replica_set_uri(creds))
        replicated = str(self.charm.is_joined)
        replica_set_name = self.charm.replica_set_name
        event.relation.data[self.charm.app]['username'] = creds['username']
        event.relation.data[self.charm.app]['password'] = creds['password']
        event.relation.data[self.charm.app]['standalone_uri'] = standalone_uri
        event.relation.data[self.charm.app]['replica_set_uri'] = replica_set_uri
        event.relation.data[self.charm.app]['replicated'] = replicated
        event.relation.data[self.charm.app]['replica_set_name'] = replica_set_name

    def on_database_relation_changed(self, event):
        """Ensure total number of databases requested are available
        """
        if not self.charm.unit.is_leader():
            return

        data = event.relation.data[event.app]
        logger.debug("SERVER REQUEST DATA {}".format(data))
        dbs = data.get('databases')
        dbs_requested = json.loads(dbs) if dbs else []
        logger.debug("SERVER REQUEST DB {}".format(dbs_requested))
        dbs_available = self.charm.mongo.databases
        logger.debug("SERVER AVAILABLE DB {}".format(dbs_available))

        missing = None
        if dbs_requested:
            if dbs_available:
                missing = list(set(dbs_requested) - set(dbs_available))
            else:
                missing = dbs_requested

        if missing:
            dbs_available.extend(missing)
            logger.debug("SERVER REQUEST RESPONSE {}".format(dbs_available))
            rel_id = event.relation.id
            creds = self.credentials(rel_id)
            self.charm.mongo.new_databases(creds, missing)
            event.relation.data[self.charm.app]['databases'] = json.dumps(dbs_available)

    def on_database_relation_broken(self, event):
        if not self.charm.unit.is_leader():
            return

        rel_id = event.relation.id
        databases = json.loads(event.relation.data[self.charm.app]['databases'])

        if rel_id in self.stored.consumers:
            creds = self.credentials(rel_id)
            self.charm.mongo.drop_user(creds["username"])
            _ = self.stored.consumers.pop(rel_id)

        if self.charm.model.config['autodelete']:
            self.charm.mongo.drop_databases(databases)

    def is_new_relation(self, rel_id):
        if rel_id in self.stored.consumers:
            return False
        else:
            return True

    def credentials(self, rel_id):
        if self.is_new_relation(rel_id):
            creds = {
                'username': self.new_username(rel_id),
                'password': MongoDB.new_password()
            }
            self.stored.consumers[rel_id] = creds
        else:
            creds = self.stored.consumers[rel_id]
        return creds

    def new_username(self, rel_id):
        username = "user-{}".format(rel_id)
        return username
