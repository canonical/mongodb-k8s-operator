# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

# The unique Charmhub library identifier, never change it
LIBID = "b74007eda21c453a89e4dcc6382aa2b3"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


class OperatorUser:
    """Operator user for MongoDB."""

    username = "operator"
    password_key = f"{username}_password"
    database = "admin"
    role = "clusterAdmin"


class MonitorUser:
    """Monitor user for MongoDB."""

    username = "monitor"
    password_key = f"{username}_password"
    database = "admin"
    role = "monitor"
    privileges = {
        "resource": {"db": "", "collection": ""},
        "actions": ["listIndexes", "listCollections", "dbStats", "dbHash", "collStats", "find"],
    }


class BackupUser:
    """Backup user for MongoDB."""

    username = "backup"


# List of system usernames needed for correct work on the charm.
CHARM_USERS = [OperatorUser.username, BackupUser.username, MonitorUser.username]


def get_password_key_name_for_user(username: str) -> str:
    """Returns the key name for the password of the user."""
    return f"{username}_password"
