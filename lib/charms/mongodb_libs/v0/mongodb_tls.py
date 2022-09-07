# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class we manage client database relations.

This class creates user and database for each application relation
and expose needed information for client connection via fields in
external relation.
"""

import re
import logging
import base64
from ops.framework import Object
from ops.charm import ActionEvent
from typing import Optional

# The unique Charmhub library identifier, never change it
LIBID = "1057f353503741a98ed79309b5be7e33"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version.
LIBPATCH = 0

logger = logging.getLogger(__name__)
TLS_RELATION = "certificates"


class MongoDBTLS(Object):
    """In this class we manage client database relations."""

    def __init__(self, charm, peer_relation):
        """Manager of MongoDB client relations."""
        super().__init__(charm, "client-relations")
        self.charm = charm
        self.peer_relation = peer_relation
        self.framework.observe(self.charm.on.set_tls_private_key_action, self._on_set_tls_private_key)

    def _on_set_tls_private_key(self, event: ActionEvent) -> None:
        """Set the TLS private key, which will be used for requesting the certificate."""
        try:
            self._request_certificate("unit", "key", event.params.get("external-key", None))
            self._request_certificate("unit", "ca", event.params.get("external-ca", None))
            self._request_certificate("unit", "cert", event.params.get("external-cert", None))
            if not self.charm.unit.is_leader():
                event.log(
                    "Only juju leader unit can set private key for the internal certificate. Skipping."
                )
                return
            self._request_certificate("app", "key", event.params.get("internal-key", None))
            self._request_certificate("app", "ca", event.params.get("internal-ca", None))
            self._request_certificate("app", "cert", event.params.get("internal-cert", None))
        except ValueError as e:
            event.fail(str(e))

    def _request_certificate(self, scope: str, file: str, param: Optional[str]):
        if not param:
            return

        key = self._parse_tls_file(param)
        self.charm.set_secret(scope, file, key.decode("utf-8"))

    @staticmethod
    def _parse_tls_file(raw_content: str) -> bytes:
        """Parse TLS files from both plain text or base64 format."""
        if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", raw_content):
            return re.sub(
                r"(-+(BEGIN|END) [A-Z ]+-+)",
                "\n\\1\n",
                raw_content,
            ).encode("utf-8")
        return base64.b64decode(raw_content)

    def get_tls_files(self, scope: str) -> (Optional[str], Optional[str]):
        """Prepare TLS files in special MongoDB way.

        MongoDB needs two files:
        — CA file should have a full chain.
        — PEM file should have private key and certificate without certificate chain.
        """
        ca = self.charm.get_secret(scope, "ca")
        chain = self.charm.get_secret(scope, "chain")
        ca_file = chain if chain else ca

        key = self.charm.get_secret(scope, "key")
        cert = self.charm.get_secret(scope, "cert")
        pem_file = key
        if cert:
            pem_file = key + "\n" + cert if key else cert

        return ca_file, pem_file
