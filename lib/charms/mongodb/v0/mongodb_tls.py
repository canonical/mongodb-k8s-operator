# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class we manage client database relations.

This class creates user and database for each application relation
and expose needed information for client connection via fields in
external relation.
"""
import base64
import logging
import re
import socket
from typing import List, Optional, Tuple

from charms.tls_certificates_interface.v1.tls_certificates import (
    CertificateAvailableEvent,
    CertificateExpiringEvent,
    TLSCertificatesRequiresV1,
    generate_csr,
    generate_private_key,
)
from ops.charm import ActionEvent, RelationBrokenEvent, RelationJoinedEvent
from ops.framework import Object
from ops.model import ActiveStatus, MaintenanceStatus, Unit

from literals import APP_SCOPE, UNIT_SCOPE

# The unique Charmhub library identifier, never change it
LIBID = "e02a50f0795e4dd292f58e93b4f493dd"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 5


logger = logging.getLogger(__name__)
TLS_RELATION = "certificates"

CA_LABEL = "ca-secret"
KEY_LABEL = "key-secret"
CERT_LABEL = "cert-secret"
CSR_LABEL = "csr-secret"
CHAIN_LABEL = "chain-secret"


class MongoDBTLS(Object):
    """In this class we manage client database relations."""

    def __init__(self, charm, peer_relation, substrate):
        """Manager of MongoDB client relations."""
        super().__init__(charm, "client-relations")
        self.charm = charm
        self.substrate = substrate
        self.peer_relation = peer_relation
        self.certs = TLSCertificatesRequiresV1(self.charm, TLS_RELATION)
        self.framework.observe(
            self.charm.on.set_tls_private_key_action, self._on_set_tls_private_key
        )
        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_joined, self._on_tls_relation_joined
        )
        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_broken, self._on_tls_relation_broken
        )
        self.framework.observe(self.certs.on.certificate_available, self._on_certificate_available)
        self.framework.observe(self.certs.on.certificate_expiring, self._on_certificate_expiring)

    def _on_set_tls_private_key(self, event: ActionEvent) -> None:
        """Set the TLS private key, which will be used for requesting the certificate."""
        logger.debug("Request to set TLS private key received.")
        try:
            self._request_certificate(UNIT_SCOPE, event.params.get("external-key", None))

            if not self.charm.unit.is_leader():
                event.log(
                    "Only juju leader unit can set private key for the internal certificate. Skipping."
                )
                return

            self._request_certificate(APP_SCOPE, event.params.get("internal-key", None))
            logger.debug("Successfully set TLS private key.")
        except ValueError as e:
            event.fail(str(e))

    def _request_certificate(self, scope: str, param: Optional[str]):
        if param is None:
            key = generate_private_key()
        else:
            key = self._parse_tls_file(param)

        csr = generate_csr(
            private_key=key,
            subject=self.get_host(self.charm.unit),
            organization=self.charm.app.name,
            sans=self._get_sans(),
            sans_ip=[str(self.charm.model.get_binding(self.peer_relation).network.bind_address)],
        )

        self.charm.set_secret(scope, KEY_LABEL, key.decode("utf-8"))
        self.charm.set_secret(scope, CSR_LABEL, csr.decode("utf-8"))
        self.charm.set_secret(scope, CERT_LABEL, None)

        if self.charm.model.get_relation(TLS_RELATION):
            self.certs.request_certificate_creation(certificate_signing_request=csr)

    @staticmethod
    def _parse_tls_file(raw_content: str) -> bytes:
        """Parse TLS files from both plain text or base64 format."""
        if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", raw_content):
            return (
                re.sub(
                    r"(-+(BEGIN|END) [A-Z ]+-+)",
                    "\\1",
                    raw_content,
                )
                .rstrip()
                .encode("utf-8")
            )
        return base64.b64decode(raw_content)

    def _on_tls_relation_joined(self, _: RelationJoinedEvent) -> None:
        """Request certificate when TLS relation joined."""
        if self.charm.unit.is_leader():
            self._request_certificate(APP_SCOPE, None)

        self._request_certificate(UNIT_SCOPE, None)

    def _on_tls_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Disable TLS when TLS relation broken."""
        logger.debug("Disabling external TLS for unit: %s", self.charm.unit.name)
        self.charm.set_secret(UNIT_SCOPE, CA_LABEL, None)
        self.charm.set_secret(UNIT_SCOPE, CERT_LABEL, None)
        self.charm.set_secret(UNIT_SCOPE, CHAIN_LABEL, None)
        if self.charm.unit.is_leader():
            logger.debug("Disabling internal TLS")
            self.charm.set_secret(APP_SCOPE, CA_LABEL, None)
            self.charm.set_secret(APP_SCOPE, CERT_LABEL, None)
            self.charm.set_secret(APP_SCOPE, CHAIN_LABEL, None)
        if self.charm.get_secret(APP_SCOPE, CERT_LABEL):
            logger.debug(
                "Defer until the leader deletes the internal TLS certificate to avoid second restart."
            )
            event.defer()
            return

        logger.info("Restarting mongod with TLS disabled.")
        self.charm.unit.status = MaintenanceStatus("disabling TLS")
        self.charm.delete_tls_certificate_from_workload()
        self.charm.restart_mongod_service()
        self.charm.unit.status = ActiveStatus()

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Enable TLS when TLS certificate available."""
        if (
            event.certificate_signing_request.rstrip()
            == self.charm.get_secret(UNIT_SCOPE, CSR_LABEL).rstrip()
        ):
            logger.debug("The external TLS certificate available.")
            scope = UNIT_SCOPE  # external crs
        elif (
            event.certificate_signing_request.rstrip()
            == self.charm.get_secret(APP_SCOPE, CSR_LABEL).rstrip()
        ):
            logger.debug("The internal TLS certificate available.")
            scope = APP_SCOPE  # internal crs
        else:
            logger.error("An unknown certificate available.")
            return

        if scope == UNIT_SCOPE or (scope == APP_SCOPE and self.charm.unit.is_leader()):
            self.charm.set_secret(
                scope, CHAIN_LABEL, "\n".join(event.chain) if event.chain is not None else None
            )
            self.charm.set_secret(scope, CERT_LABEL, event.certificate)
            self.charm.set_secret(scope, CA_LABEL, event.ca)

        if self._waiting_for_certs():
            logger.debug(
                "Defer till both internal and external TLS certificates available to avoid second restart."
            )
            event.defer()
            return

        logger.debug("Restarting mongod with TLS enabled.")

        self.charm.push_tls_certificate_to_workload()
        self.charm.unit.status = MaintenanceStatus("enabling TLS")
        self.charm.restart_mongod_service()
        self.charm.unit.status = ActiveStatus()

    def _waiting_for_certs(self):
        """Returns a boolean indicating whether additional certs are needed."""
        if not self.charm.get_secret(APP_SCOPE, CERT_LABEL):
            logger.debug("Waiting for application certificate.")
            return True
        if not self.charm.get_secret(UNIT_SCOPE, CERT_LABEL):
            logger.debug("Waiting for application certificate.")
            return True

        return False

    def _on_certificate_expiring(self, event: CertificateExpiringEvent) -> None:
        """Request the new certificate when old certificate is expiring."""
        if event.certificate.rstrip() == self.charm.get_secret(UNIT_SCOPE, CERT_LABEL).rstrip():
            logger.debug("The external TLS certificate expiring.")
            scope = UNIT_SCOPE  # external cert
        elif event.certificate.rstrip() == self.charm.get_secret(APP_SCOPE, CERT_LABEL).rstrip():
            logger.debug("The internal TLS certificate expiring.")
            if not self.charm.unit.is_leader():
                return
            scope = APP_SCOPE  # internal cert
        else:
            logger.error("An unknown certificate expiring.")
            return

        logger.debug("Generating a new Certificate Signing Request.")
        key = self.charm.get_secret(scope, KEY_LABEL).encode("utf-8")
        old_csr = self.charm.get_secret(scope, CSR_LABEL).encode("utf-8")
        new_csr = generate_csr(
            private_key=key,
            subject=self.get_host(self.charm.unit),
            organization=self.charm.app.name,
            sans=self._get_sans(),
            sans_ip=[str(self.charm.model.get_binding(self.peer_relation).network.bind_address)],
        )
        logger.debug("Requesting a certificate renewal.")

        self.certs.request_certificate_renewal(
            old_certificate_signing_request=old_csr,
            new_certificate_signing_request=new_csr,
        )

        self.charm.set_secret(scope, CSR_LABEL, new_csr.decode("utf-8"))

    def _get_sans(self) -> List[str]:
        """Create a list of DNS names for a MongoDB unit.

        Returns:
            A list representing the hostnames of the MongoDB unit.
        """
        unit_id = self.charm.unit.name.split("/")[1]
        return [
            f"{self.charm.app.name}-{unit_id}",
            socket.getfqdn(),
            f"{self.charm.app.name}-{unit_id}.{self.charm.app.name}-endpoints",
            str(self.charm.model.get_binding(self.peer_relation).network.bind_address),
        ]

    def get_tls_files(self, scope: str) -> Tuple[Optional[str], Optional[str]]:
        """Prepare TLS files in special MongoDB way.

        MongoDB needs two files:
        — CA file should have a full chain.
        — PEM file should have private key and certificate without certificate chain.
        """
        ca = self.charm.get_secret(scope, CA_LABEL)
        chain = self.charm.get_secret(scope, CHAIN_LABEL)
        ca_file = chain if chain else ca

        key = self.charm.get_secret(scope, KEY_LABEL)
        cert = self.charm.get_secret(scope, CERT_LABEL)
        pem_file = key
        if cert:
            pem_file = key + "\n" + cert if key else cert

        return ca_file, pem_file

    def get_host(self, unit: Unit):
        """Retrieves the hostname of the unit based on the substrate."""
        if self.substrate == "vm":
            return self.charm._unit_ip(unit)
        else:
            return self.charm.get_hostname_for_unit(unit)
