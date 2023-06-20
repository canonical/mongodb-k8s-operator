"""Users configuration for MongoDB."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
from typing import List

# The unique Charmhub library identifier, never change it
LIBID = "b74007eda21c453a89e4dcc6382aa2b3"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


class MongoDBUser:
    """Base class for MongoDB users."""

    _username = ""
    _password_key_name = ""
    _database_name = ""
    _roles = []
    _privileges = {}
    _mongodb_role = ""
    _hosts = []

    def get_username(self) -> str:
        """Returns the username of the user."""
        return self._username

    def get_password_key_name(self) -> str:
        """Returns the key name for the password of the user."""
        return self._password_key_name

    def get_database_name(self) -> str:
        """Returns the database of the user."""
        return self._database_name

    def get_roles(self) -> List[str]:
        """Returns the role of the user."""
        return self._roles

    def get_mongodb_role(self) -> str:
        """Returns the MongoDB role of the user."""
        return self._mongodb_role

    def get_privileges(self) -> dict:
        """Returns the privileges of the user."""
        return self._privileges

    def get_hosts(self) -> list:
        """Returns the hosts of the user."""
        return self._hosts

    @staticmethod
    def get_password_key_name_for_user(username: str) -> str:
        """Returns the key name for the password of the user."""
        if username == OperatorUser.get_username():
            return OperatorUser.get_password_key_name()
        elif username == MonitorUser.get_username():
            return MonitorUser.get_password_key_name()
        elif username == BackupUser.get_username():
            return BackupUser.get_password_key_name()
        else:
            raise ValueError(f"Unknown user: {username}")


class _OperatorUser(MongoDBUser):
    """Operator user for MongoDB."""

    _username = "operator"
    _password_key_name = f"{_username}-password"
    _database_name = "admin"
    _roles = ["default"]
    _hosts = []


class _MonitorUser(MongoDBUser):
    """Monitor user for MongoDB."""

    _username = "monitor"
    _password_key_name = f"{_username}-password"
    _database_name = ""
    _roles = ["monitor"]
    _privileges = {
        "resource": {"db": "", "collection": ""},
        "actions": ["listIndexes", "listCollections", "dbStats", "dbHash", "collStats", "find"],
    }
    _mongodb_role = "explainRole"
    _hosts = []


class _BackupUser(MongoDBUser):
    """Backup user for MongoDB."""

    _username = "backup"
    _password_key_name = f"{_username}-password"
    _database_name = ""
    _roles = ["backup"]
    _mongodb_role = "pbmAnyAction"
    _privileges = {"resource": {"anyResource": True}, "actions": ["anyAction"]}
    _hosts = ["127.0.0.1"]  # pbm cannot make a direct connection if multiple hosts are used


OperatorUser = _OperatorUser()
MonitorUser = _MonitorUser()
BackupUser = _BackupUser()

# List of system usernames needed for correct work on the charm.
CHARM_USERS = [OperatorUser.get_username(), BackupUser.get_username(), MonitorUser.get_username()]
