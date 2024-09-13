#!/usr/bin/env python3
"""Charm code for MongoDB service."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


class MongoError(Exception):
    """Common parent for Mongo errors, allowing to catch them all at once."""


class AdminUserCreationError(MongoError):
    """Raised when a commands to create an admin user on MongoDB fail."""


class ApplicationHostNotFoundError(MongoError):
    """Raised when a queried host is not in the application peers or the current host."""


class MongoSecretError(MongoError):
    """Common parent for all Mongo Secret Exceptions."""


class SecretNotAddedError(MongoSecretError):
    """Raised when a Juju 3 secret couldn't be set or re-set."""


class MissingSecretError(MongoSecretError):
    """Could be raised when a Juju 3 mandatory secret couldn't be found."""


class SecretAlreadyExistsError(MongoSecretError):
    """A secret that we want to create already exists."""
