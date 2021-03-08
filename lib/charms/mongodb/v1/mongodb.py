import json
import uuid
import logging
from ops.relation import Consumer

LIBAPI = 1
LIBPATCH = 0
logger = logging.getLogger(__name__)


class MongoConsumer(Consumer):
    def __init__(self, charm, name, consumes, multi=False):
        super().__init__(charm, name, consumes, multi)
        self.charm = charm
        self.relation_name = name

    def databases(self):
        """List of currently available databases

        Returns:
            list: list of database names
        """
        rel_id = super().stored.relation_id
        if rel_id:
            rel = self.framework.model.get_relation(self.relation_name, rel_id)
        else:
            rel = self.framework.model.get_relation(self.relation_name)

        relation_data = rel.data[rel.app]
        dbs = relation_data.get('databases')
        databases = json.loads(dbs) if dbs else []

        return databases

    def new_database(self):
        """Request creation of an additional database

        """
        if not self.charm.unit.is_leader():
            return

        rel_id = super().stored.relation_id
        if rel_id:
            rel = self.framework.model.get_relation(self.relation_name, rel_id)
        else:
            rel = self.framework.model.get_relation(self.relation_name)

        id = uuid.uuid4()
        db_name = "db-{}-{}".format(rel.id, id)
        logger.debug("CLIENT REQUEST {}".format(db_name))
        rel_data = rel.data[self.charm.app]
        dbs = rel_data.get('databases')
        dbs = json.loads(dbs) if dbs else []
        dbs.append(db_name)
        rel.data[self.charm.app]['databases'] = json.dumps(dbs)
