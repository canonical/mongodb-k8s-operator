#!/usr/bin/env python3
"""Charm code for MongoDB service."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


class AdminUserCreationError(Exception):
    """Raised when a commands to create an admin user on MongoDB fail."""

    pass


class ApplicationHostNotFoundError(Exception):
    """Raised when a queried host is not in the application peers or the current host."""

    pass
