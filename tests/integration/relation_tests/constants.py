
import yaml
from pathlib import Path

MEDIAN_REELECTION_TIME = 12
APPLICATION_APP_NAME = "application"
DATABASE_METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
PORT = 27017
DATABASE_APP_NAME = DATABASE_METADATA["name"]
FIRST_DATABASE_RELATION_NAME = "first-database"
SECOND_DATABASE_RELATION_NAME = "second-database"
MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "multiple-database-clusters"
ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "aliased-multiple-database-clusters"
ANOTHER_DATABASE_APP_NAME = "another-database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME, ANOTHER_DATABASE_APP_NAME]
TEST_APP_CHARM_PATH = "tests/integration/relation_tests/application-charm"
