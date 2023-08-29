#!/usr/bin/env python3
"""Charm code for MongoDB service."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


class MongoError(Exception):
    """Common parent for Mongo errors, allowing to catch them all at once."""

    pass


class AdminUserCreationError(MongoError):
    """Raised when a commands to create an admin user on MongoDB fail."""

    pass


class ApplicationHostNotFoundError(MongoError):
    """Raised when a queried host is not in the application peers or the current host."""

    pass


class MongoSecretError(MongoError):
    """Common parent for all Mongo Secret Exceptions."""

    pass


class SecretNotAddedError(MongoSecretError):
    """Raised when a Juju 3 secret couldn't be set or re-set."""

    pass


class MissingSecretError(MongoSecretError):
    """Could be raised when a Juju 3 mandatory secret couldn't be found."""

    pass
