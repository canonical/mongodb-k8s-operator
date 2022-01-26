import json
import logging

from ops.framework import Object
from mongoserver import MongoDB

logger = logging.getLogger(__name__)

# Name of the db relation
DB_RELATION_NAME = "database"


class MongoProvider(Object):

    def __init__(self, charm, name, *args, **kwargs):
        """Manager of MongoDB relations.

        Args:

            charm: a :class:`CharmBase` object representing the
                MongoDB charm. This is usually self.
            name: string name of relation being managed.
        """
        super().__init__(charm, name, *args, **kwargs)
        self.charm = charm
        events = self.charm.on[name]
        self.framework.observe(events.relation_joined,
                               self._on_database_relation_joined)
        self.framework.observe(events.relation_changed,
                               self._on_database_relation_changed)
        self.framework.observe(events.relation_broken,
                               self._on_database_relation_broken)

    ##############################################
    #               RELATIONS                    #
    ##############################################
    def _on_database_relation_joined(self, event):
        """Handle relation joined events.

        When a new relation joins the :class:`MongoProvider` sets relation
        data, that the related charm can use for accessing the MongoDB
        database.

        Args:
            event: a :class:`EventBase` object.
        """
        if not self.charm.unit.is_leader():
            return

        self._create_new_user(event.relation)

    def _on_database_relation_changed(self, event):
        """Ensure total number of databases requested are available.

        Where there is any change to relation data
        :class:`MongoProvider` checks if any new databases have been
        requested. If so these new databases are "created", the
        requesting user (charm) is granted access to these databases,
        and provided the names of the databases.

        Args:
            event: a :class:`EventBase` object.
        """
        if not self.charm.unit.is_leader():
            return

        data = event.relation.data[event.app]
        dbs = data.get('databases')
        dbs_requested = json.loads(dbs) if dbs else []
        dbs_available = self.charm.mongo.databases

        missing = None
        if dbs_requested:
            if dbs_available:
                missing = list(set(dbs_requested) - set(dbs_available))
            else:
                missing = dbs_requested

        if missing:
            logger.debug("creating new databases : {}".format(missing))
            dbs_available.extend(missing)
            rel_id = event.relation.id
            creds = self.credentials(rel_id)
            self.charm.mongo.new_databases(creds, missing)
            event.relation.data[self.charm.app]['databases'] = json.dumps(dbs_available)

    def _on_database_relation_broken(self, event):
        """Handle relation broken events.

        When a relation is broken :class:`MongoProvider` ensures that
        database access credentials allocated for that relation is
        removed. Further if the configuration option "autodelete" was
        set then the database associated with that relation is also
        removed.

        Args:
            event: a :class:`EventBase` object.
        """
        if not self.charm.unit.is_leader():
            return

        rel_id = event.relation.id
        databases = json.loads(
            event.relation.data[self.charm.app].get('databases', '[]'))

        consumers = self.consumers()
        if rel_id in consumers:
            creds = self.credentials(rel_id)
            self.charm.mongo.drop_user(creds["username"])
            _ = consumers.pop(rel_id)
            self.charm.peers.data[self.charm.app]['consumers'] = json.dumps(consumers)

        if self.charm.model.config['autodelete'] and databases:
            self.charm.mongo.drop_databases(databases)

    def _create_new_user(self, relation):
        rel_id = relation.id
        creds = self.credentials(rel_id)
        self.charm.mongo.new_user(creds)
        replica_set_uri = "{}".format(self.charm.mongo.replica_set_uri(creds))
        replica_set_name = self.charm.replica_set_name
        relation.data[self.charm.app]['username'] = creds['username']
        relation.data[self.charm.app]['password'] = creds['password']
        relation.data[self.charm.app]['replica_set_uri'] = replica_set_uri
        relation.data[self.charm.app]['replica_set_name'] = replica_set_name

    def update_all_db_relations(self):
        relations = self.model.relations[DB_RELATION_NAME]
        if not relations:
            return

        # Only treat the new relations.
        for relation in relations:
            if self.is_new_relation(relation.id):
                self._create_new_user(relation)

    def is_new_relation(self, rel_id):
        """Has this relation never been seen before.

        Args:
           rel_id: integer id of relation

        Retruns:
           True if relation was not seen before.
        """
        if rel_id in self.consumers():
            return False
        else:
            return True

    def credentials(self, rel_id):
        """Fetch credentials associated with a relation.

        Args:
           rel_id: integer id of relation

        Returns:
           A dictionary with keys "username" and "password".
        """
        consumers = self.consumers()

        if self.is_new_relation(rel_id):
            creds = {
                'username': self.new_username(rel_id),
                'password': MongoDB.new_password()
            }
            consumers[rel_id] = creds

            self.charm.peers.data[self.charm.app]['consumers'] = json.dumps(consumers)
        else:
            creds = consumers[rel_id]
        return creds

    def new_username(self, rel_id):
        """Create a new username

        Args:
           rel_id: integer id of relation

        Returns:
           A string user name.
        """
        username = "user-{}".format(rel_id)
        return username

    def consumers(self):
        """Fetch current set of consumers

        Returns:
            A dictionary where the keys are relation IDs of consumers
            and the values are the consumer credentials.
        """
        consumers = json.loads(self.charm.peers.data[self.charm.app].get('consumers', "{}"))
        return consumers
