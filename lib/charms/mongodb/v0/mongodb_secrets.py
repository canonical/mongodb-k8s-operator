"""Secrets related helper classes/functions."""
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

from typing import Dict, Optional

from ops import Secret, SecretInfo
from ops.charm import CharmBase
from ops.model import SecretNotFoundError

from config import Config
from exceptions import SecretAlreadyExistsError

# The unique Charmhub library identifier, never change it

# The unique Charmhub library identifier, never change it
LIBID = "87456e41c7594240b92b783a648592b5"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

APP_SCOPE = Config.Relations.APP_SCOPE
UNIT_SCOPE = Config.Relations.UNIT_SCOPE
Scopes = Config.Relations.Scopes


def generate_secret_label(charm: CharmBase, scope: Scopes) -> str:
    """Generate unique group_mappings for secrets within a relation context."""
    members = [charm.app.name, scope]
    return f"{'.'.join(members)}"


# Secret cache


class CachedSecret:
    """Locally cache a secret.

    The data structure is precisely re-using/simulating as in the actual Secret Storage
    """

    def __init__(self, charm: CharmBase, label: str, secret_uri: Optional[str] = None):
        self._secret_meta = None
        self._secret_content = {}
        self._secret_uri = secret_uri
        self.label = label
        self.charm = charm

    def add_secret(self, content: Dict[str, str], scope: Scopes) -> Secret:
        """Create a new secret."""
        if self._secret_uri:
            raise SecretAlreadyExistsError(
                "Secret is already defined with uri %s", self._secret_uri
            )

        if scope == Config.APP_SCOPE:
            secret = self.charm.app.add_secret(content, label=self.label)
        else:
            secret = self.charm.unit.add_secret(content, label=self.label)
        self._secret_uri = secret.id
        self._secret_meta = secret
        return self._secret_meta

    @property
    def meta(self) -> Optional[Secret]:
        """Getting cached secret meta-information."""
        if not self._secret_meta:
            if not (self._secret_uri or self.label):
                return
            try:
                self._secret_meta = self.charm.model.get_secret(label=self.label)
            except SecretNotFoundError:
                if self._secret_uri:
                    self._secret_meta = self.charm.model.get_secret(
                        id=self._secret_uri, label=self.label
                    )
        return self._secret_meta

    def get_content(self) -> Dict[str, str]:
        """Getting cached secret content."""
        if not self._secret_content:
            if self.meta:
                self._secret_content = self.meta.get_content()
        return self._secret_content

    def set_content(self, content: Dict[str, str]) -> None:
        """Setting cached secret content."""
        if self.meta:
            self.meta.set_content(content)
            self._secret_content = content

    def get_info(self) -> Optional[SecretInfo]:
        """Wrapper function for get the corresponding call on the Secret object if any."""
        if self.meta:
            return self.meta.get_info()


class SecretCache:
    """A data structure storing CachedSecret objects."""

    def __init__(self, charm):
        self.charm = charm
        self._secrets: Dict[str, CachedSecret] = {}

    def get(self, label: str, uri: Optional[str] = None) -> Optional[CachedSecret]:
        """Getting a secret from Juju Secret store or cache."""
        if not self._secrets.get(label):
            secret = CachedSecret(self.charm, label, uri)
            if secret.meta:
                self._secrets[label] = secret
        return self._secrets.get(label)

    def add(self, label: str, content: Dict[str, str], scope: Scopes) -> CachedSecret:
        """Adding a secret to Juju Secret."""
        if self._secrets.get(label):
            raise SecretAlreadyExistsError(f"Secret {label} already exists")

        secret = CachedSecret(self.charm, label)
        secret.add_secret(content, scope)
        self._secrets[label] = secret
        return self._secrets[label]
