import json
import uuid
from ops.relation import ConsumerBase

LIBAPI = 1
LIBPATCH = 0


class MongoConsumer(ConsumerBase):
    def __init__(self, charm, name, consumes, multi=False):
        super().__init__(charm, name, consumes, multi)
        self.charm = charm
        self.relation_name = name

    def provider_ids(self):
        """Return relation IDs of all database providers
        """
        id_list = []
        for rel in self.charm.model.relations[self.relation_name]:
            id_list.append(rel.id)
        return id_list

    def credentials(self, rel_id=None):
        """Get authentication credentials for particular provider

        Args:
            rel_id: id of relation for which credentials are required.

        Returns:
            dictionary containing "username" and "password" credentials
            if available, otherwise an empty dictionary is returned.
        """

        rel = self.framework.model.get_relation(self.relation_name, rel_id)
        relation_data = rel.data[rel.app]
        username = relation_data.get('username')
        password = relation_data.get('password')
        replica_set_uri = relation_data.get('replica_set_uri')
        replicated = relation_data.get('replicated')

        credentials = {}
        if username and password:
            credentials = {"username": username,
                           "password": password,
                           "replica_set_uri": replica_set_uri,
                           "replicated": replicated}
        return credentials

    def databases(self, rel_id=None):
        """List of currently available databases

        Args:
            rel_id: id of relation for which database list is required.
                This is optional in single relation mode but if it is
                not provided in multi mode then TooManyRelatedAppsError
                exception is raised.

        Raises:
            TooManyRelatedAppsError if relation id is not provided and
            multiple relation of the same name are present

        Returns:
            list: list of database names
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)
        relation_data = rel.data[rel.app]
        dbs = relation_data.get('databases')
        databases = json.loads(dbs) if dbs else []

        return databases

    def new_database(self, rel_id=None):
        """Request creation of an additional database

        Args:
            rel_id: id of relation for which database list is required.
                This is optional in single relation mode but if it is
                not provided in multi mode then TooManyRelatedAppsError
                exception is raised.

        Raises:
            TooManyRelatedAppsError if relation id is not provided and
            multiple relation of the same name are present
        """
        if not self.charm.unit.is_leader():
            return

        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        id = uuid.uuid4()
        db_name = "db-{}-{}".format(rel.id, id)
        rel_data = rel.data[self.charm.app]
        dbs = rel_data.get('databases')
        dbs = json.loads(dbs) if dbs else []
        dbs.append(db_name)
        rel.data[self.charm.app]['databases'] = json.dumps(dbs)
