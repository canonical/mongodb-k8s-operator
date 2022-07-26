# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

"""Library for the tls-certificates relation.

This library contains the Requires and Provides classes for handling
the tls-certificates interface.

## Getting Started

From a charm directory, fetch the library using `charmcraft`:

```shell
charmcraft fetch-lib charms.tls_certificates_interface.v0.tls_certificates
```

You will also need to add the following library to the charm's `requirements.txt` file:
- jsonschema

### Provider charm
Example:
```python
from charms.tls_certificates_interface.v0.tls_certificates import (
    Cert,
    TLSCertificatesProvides,
)
from ops.charm import CharmBase


class ExampleProviderCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.tls_certificates = TLSCertificatesProvides(self, "certificates")
        self.framework.observe(
            self.tls_certificates.on.certificates_request, self._on_certificate_request
        )

    def _on_certificate_request(self, event):
        common_name = event.common_name
        sans = event.sans
        cert_type = event.cert_type
        certificate = self._generate_certificate(common_name, sans, cert_type)

        self.tls_certificates.set_relation_certificate(
            certificate=certificate, relation_id=event.relation.id
        )

    def _generate_certificate(self, common_name: str, sans: list, cert_type: str) -> Cert:
        return Cert(
            common_name=common_name, cert="whatever cert", key="whatever key", ca="whatever ca"
        )
```

### Requirer charm
Example:

```python
from charms.tls_certificates_interface.v0.tls_certificates import TLSCertificatesRequires
from ops.charm import CharmBase


class ExampleRequirerCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)

        self.tls_certificates = TLSCertificatesRequires(self, "certificates")
        self.framework.observe(
            self.tls_certificates.on.certificate_available, self._on_certificate_available
        )
        self.tls_certificates.request_certificate(
            cert_type="client",
            common_name="whatever common name",
        )

    def _on_certificate_available(self, event):
        certificate_data = event.certificate_data
        print(certificate_data["common_name"])
        print(certificate_data["key"])
        print(certificate_data["ca"])
        print(certificate_data["cert"])
```

"""
import json
import logging
from typing import List, Literal, TypedDict

from jsonschema import exceptions, validate  # type: ignore[import]
from ops.charm import CharmEvents
from ops.framework import EventBase, EventSource, Object

# The unique Charmhub library identifier, never change it
LIBID = "afd8c2bccf834997afce12c2706d2ede"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 23

REQUIRER_JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "description": "The root schema comprises the entire JSON document. It contains the data bucket content and format for the requirer of the tls-certificates relation to ask TLS certificates to the provider.",  # noqa: E501
    "examples": [{"cert_requests": [{"common_name": "canonical.com"}]}],
    "properties": {"common_name": {"type": "string"}, "sans": {"type": "array"}},
    "anyOf": [
        {
            "required": ["cert_requests"],
            "type": "object",
            "properties": {
                "cert_requests": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sans": {"type": "array", "items": {"type": "string"}},
                            "common_name": {"type": "string"},
                        },
                        "required": ["common_name"],
                    },
                }
            },
        },
        {
            "type": "object",
            "required": ["client_cert_requests"],
            "properties": {
                "client_cert_requests": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sans": {"type": "array", "items": {"type": "string"}},
                            "common_name": {"type": "string"},
                        },
                        "required": ["common_name"],
                    },
                }
            },
        },
    ],
}

PROVIDER_JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "description": "The root schema comprises the entire JSON document. It contains the data bucket content and format for the provider of the tls-certificates relation to provide certificates to the requirer.",  # noqa: E501
    "example": [
        {
            "whatever.com": {
                "key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA1CHw0NwHmuu/1ych0G/vlE0ArsPo3meWvG5u0rlg1xoGwdbh\nJeG7DIDtBlU71NFVs/DLdAtx2MwWg0rKYXGzBex+XaI4WLowLmD+KTk8ZCWvBHXq\nQ+N5Tc7CFlBnVW7xPqAE/HGFTgAhH+vq1lYKfXNpJMlXf3EuaNycmVwJgyKdh9d8\njzEqa54YWtH/E2guv+Rb9atGdmVHgLhNPgcwIuIODYJpQLjawnT2Lf8Z7MdTqVdV\n7slorOnz2mHslqC5KPnvDXIrVmrwhltf9DcWpxH8ZzkAfOdEFA53ioBBwaIPYoy1\nUvBJRnrbjhVoTq6XUM8BheqzfPppKnpnJPE2jwIDAQABAoIBAAjN39DLUQV9A1lK\npnygKLFfAMhAGUohwn/PlYnpZ7uFuQISiQWpeLnsH+pDX1hV19jABbGrR+5Xiheo\n4v1oWqXESvpX4T7Ne3JxVBsh5P/DEKB+xFpM9pvkGOoULDW/hQO0YICZtY6nMrjA\ncd6zc3wBbju4n4kKiYKQpW84Aq0Oj1OXIfo48F776MTm3WNAgdpHC5CYmQdUu20A\nAnmS52XSOcva9dZrs29kQM7iA2ssefLy+yQLR5jCiLvrmvjZtSCpDryfDC2A2nny\n4dMhyLCezTVcCy8kTTTgaOh+kxgbCwQJNT6xcOIB4fYBVUn0wd+UiCqxGVIehSGd\nLSlyMAECgYEA/Eax/FJ3SQa28z+to3i7zy03f2porDs+uKVyQcfZlxniEzoEKYPJ\nFdG4XtCjgre5YCUPeWCJgUEcc2x5XdP4T33bN9cMOgz+g4hpP6I2wj9/QV3tnAFL\nhlOIFEkDlBIURXIORU115D2CNBUWfGQAsJQeKE4YncaTWO9Gn08oAEECgYEA10OM\n4f31HmO/QyW3uARIgTF2GZDkVPeYvuG6DBDHGznJzdSNJgIP+Jotg344vR555IMQ\nUtvzg6cVpLdJypOI+sNiiYBlHUgyc4E0yGnBedNKpxF1gYw4sljX6fJ6FPaBcq9X\n6sKXC90KWe4lAvLE4c+qGInFqPxw7BPVBnV4gs8CgYB39jYUwjIu6557tUAgh/zm\n252UXUlA/TsqGqJmXV4+1/QFKIVqKFyqn3uIurXGJw9jhLwC/8DjUc7xpBdiYrWl\nNzfTKdOKlzs/2NITjFN1szQUJVIj6Qm86mO/Iakt9BrnmwDmO5tf2U/c7Fow9GzP\nit98UwapoA/ZLo7qmn1vAQKBgH+YHKO/4lD3EuF8M9+xOkDJzpTs20q50CIkriCE\nuWAb6tBEUr3arxjOWnf8kykWLW4TedODaF365ctSkTywIptwwLF8F3M53h200lKQ\nzQunADLzGFGHifu8yY50GYTfcG9IG7adTObNSFtx2yJaP/URIGOXFkBKEaz9PGGt\ns5blAoGBAI20XD6yAL8cD5EN0pZ2eHDQbF/g8ML8zxJG4vCV2ElhfH+L4Q2zXAYp\nDAGRj+z928KUM+OIjyts7RWoQm0/5Bf9VuvrC0o3H0pw4rmNyW1VEJWR7LLgA647\n7O8CvHALrL7aMh6XWNmDMSrO63nYN5JzRpxJXXtPqmqNSd6cUewP\n-----END RSA PRIVATE KEY-----",  # noqa: E501
                "cert": "-----BEGIN CERTIFICATE-----\nMIIDajCCAlKgAwIBAgIUaKRAIcZmkNziPb6FpgfShKTHj/wwDQYJKoZIhvcNAQEL\nBQAwPTE7MDkGA1UEAxMyVmF1bHQgUm9vdCBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkg\nKGNoYXJtLXBraS1sb2NhbCkwHhcNMjIwNTI2MDAzMzI0WhcNMjMwNTI1MjMzMzU0\nWjAVMRMwEQYDVQQDEwpibGFibGEuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\nMIIBCgKCAQEA1CHw0NwHmuu/1ych0G/vlE0ArsPo3meWvG5u0rlg1xoGwdbhJeG7\nDIDtBlU71NFVs/DLdAtx2MwWg0rKYXGzBex+XaI4WLowLmD+KTk8ZCWvBHXqQ+N5\nTc7CFlBnVW7xPqAE/HGFTgAhH+vq1lYKfXNpJMlXf3EuaNycmVwJgyKdh9d8jzEq\na54YWtH/E2guv+Rb9atGdmVHgLhNPgcwIuIODYJpQLjawnT2Lf8Z7MdTqVdV7slo\nrOnz2mHslqC5KPnvDXIrVmrwhltf9DcWpxH8ZzkAfOdEFA53ioBBwaIPYoy1UvBJ\nRnrbjhVoTq6XUM8BheqzfPppKnpnJPE2jwIDAQABo4GJMIGGMA4GA1UdDwEB/wQE\nAwIDqDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwHQYDVR0OBBYEFASO\nWjQVWEhsucKdwUeyq4RRwmygMB8GA1UdIwQYMBaAFPOJYJ5nPJg3UVlfKPxggdig\n/n+nMBUGA1UdEQQOMAyCCmJsYWJsYS5jb20wDQYJKoZIhvcNAQELBQADggEBAGq8\nVrNFmTkf9jG3R8yD1HIZp0cbDacF25SHSYS3+M32BlITve0OOA0CzW3OLrXnCTp7\nLoSMWpWM5TFeJNl/lV4bC8izXA3hsf3bHXERkEGfjuTUmjK8QodvAs/ueoaD1E/Y\n0b9w3Qb3+dbs9joU/2XltvOcTPmtjTsfkMQ12sFozzLn4LVZTDe9Pmt2YXcnv+nd\navU0bCVNWYLc/6AHImtKYrziBBk+mfwYkPFFdwjpwHVPuCTMsZSBY8TrSuuk79w4\nBTRXzEBsCizprRGFRmZnFCA+SbMkh2PWpKziujODdGMZjUWtgFI2AGwMtIk2KXLK\nD/8jxyBXHnvBJ5S3vNo=\n-----END CERTIFICATE-----",  # noqa: E501
            },
            "ca": "-----BEGIN CERTIFICATE-----\nMIIDazCCAlOgAwIBAgIUcmcehy58Qe+g00FUC4WrGYqOBwwwDQYJKoZIhvcNAQEL\nBQAwPTE7MDkGA1UEAxMyVmF1bHQgUm9vdCBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkg\nKGNoYXJtLXBraS1sb2NhbCkwHhcNMjIwNTI2MDAxNzIwWhcNMzIwNTIyMjMxNzUw\nWjA9MTswOQYDVQQDEzJWYXVsdCBSb290IENlcnRpZmljYXRlIEF1dGhvcml0eSAo\nY2hhcm0tcGtpLWxvY2FsKTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB\nAMQ5VG4o/tKjAr+1p0fH4XP+IPlGJTubDuMC1S7KwwxqyXmW/PXhIxC4zPPiRodK\nJ1sBtAla0fVQ5xRjou/FTdKtHbXMm9Nh4fVauFu7HMLHvDjBwZKl26eZ536QhOO9\nggjs/Gx9pYoWmTKqDGbGZRgKx4zjYmMNYdY7VfpLqmzYEMMPy4mQRHajWo7ksdMh\nev/ZU0PSyY4Vfk7+a8O/gTGebRYZzUAVMJX+7RMySqPvtI7Cm9KrYZFLDoLT/yKW\neOi/oynbCTnCKllR9GsmjHtO/bjoE4Ggmn4zWHSZSaoe7deG27CVHT/gU+DLQhQi\n/MNbCItkUzYKU6YO+0INXp8CAwEAAaNjMGEwDgYDVR0PAQH/BAQDAgEGMA8GA1Ud\nEwEB/wQFMAMBAf8wHQYDVR0OBBYEFPOJYJ5nPJg3UVlfKPxggdig/n+nMB8GA1Ud\nIwQYMBaAFPOJYJ5nPJg3UVlfKPxggdig/n+nMA0GCSqGSIb3DQEBCwUAA4IBAQAj\nHzDsi3GNtp6mPAt9eUjR69WPdS8GgI4ypqIaKjS/r8lwEB1y9FT50NgYhb+nH/2y\nj5ajQEF/Mf9GBJOpFtPWpULxPra5EeCVpMS9sCP1BFS3Tq1/p09kb5kGNzJPQ5u1\nNJDJMAhzUHZcxCnqNBrRKhvtWKNZygvcZuV2nypN+vvtMXlZv5GMrYGpOUomUGza\nviGfaLiGdNeWBXElKe1slutUXXTkLOMS7rLQ5RziDrVxXn9uuE1lrTovEACvrP1Z\n0BFJIuGTn699OGevx44u4gO4qIkzpGeQ1gAnSdgq2HZpxSAYdi2ay8MSHbOfsLY0\nAIdVl46lbjsmIh+vvFyI\n-----END CERTIFICATE-----",  # noqa: E501
            "chain": "-----BEGIN CERTIFICATE-----\nMIIDazCCAlOgAwIBAgIUcmcehy58Qe+g00FUC4WrGYqOBwwwDQYJKoZIhvcNAQEL\nBQAwPTE7MDkGA1UEAxMyVmF1bHQgUm9vdCBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkg\nKGNoYXJtLXBraS1sb2NhbCkwHhcNMjIwNTI2MDAxNzIwWhcNMzIwNTIyMjMxNzUw\nWjA9MTswOQYDVQQDEzJWYXVsdCBSb290IENlcnRpZmljYXRlIEF1dGhvcml0eSAo\nY2hhcm0tcGtpLWxvY2FsKTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB\nAMQ5VG4o/tKjAr+1p0fH4XP+IPlGJTubDuMC1S7KwwxqyXmW/PXhIxC4zPPiRodK\nJ1sBtAla0fVQ5xRjou/FTdKtHbXMm9Nh4fVauFu7HMLHvDjBwZKl26eZ536QhOO9\nggjs/Gx9pYoWmTKqDGbGZRgKx4zjYmMNYdY7VfpLqmzYEMMPy4mQRHajWo7ksdMh\nev/ZU0PSyY4Vfk7+a8O/gTGebRYZzUAVMJX+7RMySqPvtI7Cm9KrYZFLDoLT/yKW\neOi/oynbCTnCKllR9GsmjHtO/bjoE4Ggmn4zWHSZSaoe7deG27CVHT/gU+DLQhQi\n/MNbCItkUzYKU6YO+0INXp8CAwEAAaNjMGEwDgYDVR0PAQH/BAQDAgEGMA8GA1Ud\nEwEB/wQFMAMBAf8wHQYDVR0OBBYEFPOJYJ5nPJg3UVlfKPxggdig/n+nMB8GA1Ud\nIwQYMBaAFPOJYJ5nPJg3UVlfKPxggdig/n+nMA0GCSqGSIb3DQEBCwUAA4IBAQAj\nHzDsi3GNtp6mPAt9eUjR69WPdS8GgI4ypqIaKjS/r8lwEB1y9FT50NgYhb+nH/2y\nj5ajQEF/Mf9GBJOpFtPWpULxPra5EeCVpMS9sCP1BFS3Tq1/p09kb5kGNzJPQ5u1\nNJDJMAhzUHZcxCnqNBrRKhvtWKNZygvcZuV2nypN+vvtMXlZv5GMrYGpOUomUGza\nviGfaLiGdNeWBXElKe1slutUXXTkLOMS7rLQ5RziDrVxXn9uuE1lrTovEACvrP1Z\n0BFJIuGTn699OGevx44u4gO4qIkzpGeQ1gAnSdgq2HZpxSAYdi2ay8MSHbOfsLY0\nAIdVl46lbjsmIh+vvFyI\n-----END CERTIFICATE-----",  # noqa: E501
            "unit_name": "whatever_unit_name",
        }
    ],
    "properties": {
        "ca": {"type": "string"},
        "chain": {"type": "string"},
        "unit_name": {"type": "string"},
    },
    "required": ["ca", "chain"],
}

logger = logging.getLogger(__name__)


class Cert(TypedDict):
    """Certificate data object."""

    common_name: str
    cert: str
    key: str
    ca: str


class CertificateAvailableEvent(EventBase):
    """Charm Event triggered when a TLS certificate is available."""

    def __init__(self, handle, certificate_data: Cert):
        super().__init__(handle)
        self.certificate_data = certificate_data

    def snapshot(self) -> dict:
        """Returns snapshot."""
        return {"certificate_data": self.certificate_data}

    def restore(self, snapshot: dict):
        """Restores snapshot."""
        self.certificate_data = snapshot["certificate_data"]


class CertificateRequestEvent(EventBase):
    """Charm Event triggered when a TLS certificate is required."""

    def __init__(self, handle, common_name: str, sans: str, cert_type: str, relation_id: int):
        super().__init__(handle)
        self.common_name = common_name
        self.sans = sans
        self.cert_type = cert_type
        self.relation_id = relation_id

    def snapshot(self) -> dict:
        """Returns snapshot."""
        return {
            "common_name": self.common_name,
            "sans": self.sans,
            "cert_type": self.cert_type,
            "relation_id": self.relation_id,
        }

    def restore(self, snapshot: dict):
        """Restores snapshot."""
        self.common_name = snapshot["common_name"]
        self.sans = snapshot["sans"]
        self.cert_type = snapshot["cert_type"]
        self.relation_id = snapshot["relation_id"]


def _load_relation_data(raw_relation_data: dict) -> dict:
    """Loads relation data from the relation data bag.

    Json loads all data.

    Args:
        raw_relation_data: Relation data from the databag

    Returns:
        dict: Relation data in dict format.
    """
    certificate_data = dict()
    for key in raw_relation_data:
        try:
            certificate_data[key] = json.loads(raw_relation_data[key])
        except json.decoder.JSONDecodeError:
            certificate_data[key] = raw_relation_data[key]
    return certificate_data


class CertificatesProviderCharmEvents(CharmEvents):
    """List of events that the TLS Certificates provider charm can leverage."""

    certificate_request = EventSource(CertificateRequestEvent)


class CertificatesRequirerCharmEvents(CharmEvents):
    """List of events that the TLS Certificates requirer charm can leverage."""

    certificate_available = EventSource(CertificateAvailableEvent)


class TLSCertificatesProvides(Object):
    """TLS certificates provider class to be instantiated by TLS certificates providers."""

    on = CertificatesProviderCharmEvents()

    def __init__(self, charm, relationship_name: str):
        super().__init__(charm, relationship_name)
        self.framework.observe(
            charm.on[relationship_name].relation_changed, self._on_relation_changed
        )
        self.charm = charm
        self.relationship_name = relationship_name

    @staticmethod
    def _relation_data_is_valid(certificates_data: dict) -> bool:
        """Uses JSON schema validator to validate relation data content.

        Args:
            certificates_data (dict): Certificate data dictionary as retrieved from relation data.

        Returns:
            bool: True/False depending on whether the relation data follows the json schema.
        """
        try:
            validate(instance=certificates_data, schema=REQUIRER_JSON_SCHEMA)
            return True
        except exceptions.ValidationError:
            return False

    def set_relation_certificate(self, certificate: Cert, relation_id: int) -> None:
        """Adds certificates to relation data.

        Args:
            certificate (Cert): Certificate object
            relation_id (int): Juju relation ID

        Returns:
            None
        """
        certificates_relation = self.model.get_relation(
            relation_name=self.relationship_name, relation_id=relation_id
        )
        relation_data = certificates_relation.data[self.model.unit]  # type: ignore[union-attr]

        current_ca = relation_data.get("ca")
        current_chain = relation_data.get("chain")
        if not current_ca:
            relation_data["ca"] = certificate["ca"]
        if not current_chain:
            relation_data["chain"] = certificate["ca"]
        certificate_dict = {"key": certificate["key"], "cert": certificate["cert"]}
        relation_data[certificate["common_name"]] = json.dumps(certificate_dict)

        certificate_dict = {"key": certificate["key"], "cert": certificate["cert"]}
        relation_data[certificate["common_name"]] = json.dumps(certificate_dict)

    def _on_relation_changed(self, event) -> None:
        """Handler triggerred on relation changed event.

        Looks at cert_requests and client_cert_requests fields in relation data and emit
        certificate request events for each entry.

        Args:
            event: Juju event

        Returns:
            None
        """
        relation_data = _load_relation_data(event.relation.data[event.unit])
        if not relation_data:
            logger.info("No relation data - Deferring")
            return
        if not self._relation_data_is_valid(relation_data):
            logger.warning("Relation data did not pass JSON Schema validation - Deferring")
            return
        for server_cert_request in relation_data.get("cert_requests", {}):
            self.on.certificate_request.emit(
                common_name=server_cert_request.get("common_name"),
                sans=server_cert_request.get("sans"),
                cert_type="server",
                relation_id=event.relation.id,
            )
        for client_cert_requests in relation_data.get("client_cert_requests", {}):
            self.on.certificate_request.emit(
                common_name=client_cert_requests.get("common_name"),
                sans=client_cert_requests.get("sans"),
                cert_type="client",
                relation_id=event.relation.id,
            )


class TLSCertificatesRequires(Object):
    """TLS certificates requirer class to be instantiated by TLS certificates requirers."""

    on = CertificatesRequirerCharmEvents()

    def __init__(
        self,
        charm,
        relationship_name: str,
        cert_type: Literal["client", "server"] = None,
        common_name: str = None,
        sans: list = None,
    ):
        super().__init__(charm, relationship_name)
        self.framework.observe(
            charm.on[relationship_name].relation_changed, self._on_relation_changed
        )
        self.relationship_name = relationship_name
        self.charm = charm
        self.common_name = common_name
        self.sans = sans
        self.cert_type = cert_type

        if cert_type is not None and common_name is not None:
            self.framework.observe(
                charm.on[relationship_name].relation_joined, self._on_relation_joined
            )

    def _on_relation_joined(self, event) -> None:
        """Handler triggered on certificates relation joined event.

        Here insert the certificate request.

        Args:
            event: Juju event

        Returns:
            None
        """
        self.request_certificate(
            cert_type=self.cert_type,
            common_name=self.common_name,
            sans=self.sans,
        )

    def request_certificate(
        self,
        cert_type: Literal["client", "server"],
        common_name: str,
        sans: list = None,
    ) -> None:
        """Request TLS certificate to provider charm.

        Args:
            cert_type (str): Certificate type: "client" or "server".
                Specifies if certificates are flagged for server or client authentication use.
                See RFC 5280 Section 4.2.1.12 for information about the Extended Key Usage field.
            common_name (str): The requested CN for the certificate.
            sans (list): Subject Alternative Name

        Returns:
            None
        """
        if not sans:
            sans = []
        logger.info("Received request to create certificate")
        relation = self.model.get_relation(self.relationship_name)
        if not relation:
            message = (
                f"Relation {self.relationship_name} does not exist - "
                f"The certificate request can't be completed"
            )
            logger.error(message)
            raise RuntimeError(message)
        relation_data = _load_relation_data(relation.data[self.model.unit])
        certificate_key_mapping = {"client": "client_cert_requests", "server": "cert_requests"}
        new_certificate_request = {"common_name": common_name, "sans": sans}
        if certificate_key_mapping[cert_type] in relation_data:
            certificate_request_list = relation_data[certificate_key_mapping[cert_type]]
            if new_certificate_request in certificate_request_list:
                logger.info("Request was already made - Doing nothing")
                return
            certificate_request_list.append(new_certificate_request)
        else:
            certificate_request_list = [new_certificate_request]
        relation.data[self.model.unit][certificate_key_mapping[cert_type]] = json.dumps(
            certificate_request_list
        )
        logger.info("Certificate request sent to provider")

    @staticmethod
    def _relation_data_is_valid(certificates_data: dict) -> bool:
        """Checks whether relation data is valid based on json schema.

        Args:
            certificates_data: Certificate data in dict format.

        Returns:
            bool: Whether relation data is valid.
        """
        try:
            validate(instance=certificates_data, schema=PROVIDER_JSON_SCHEMA)
            return True
        except exceptions.ValidationError:
            return False

    @staticmethod
    def _parse_certificates_from_relation_data(relation_data: dict) -> List[Cert]:
        """Loops over all relation data and returns list of Cert objects.

        Args:
            relation_data: Relation data json formatted.

        Returns:
            list: List of certificates
        """
        certificates = []
        ca = relation_data.pop("ca")
        relation_data.pop("chain")
        for key in relation_data:
            if type(relation_data[key]) == dict:
                private_key = relation_data[key].get("key")
                certificate = relation_data[key].get("cert")
                if private_key and certificate:
                    certificates.append(
                        Cert(common_name=key, key=private_key, cert=certificate, ca=ca)
                    )
        return certificates

    def get_certificates_for_common_name(
        self, common_name: str, departed_id: int = None
    ) -> List[Cert]:
        """Loops over all relations and returns list of Cert objects.

        Args:
            common_name: return certificates for specified the common name.
            departed_id: don't return certificates from departed relation. The argument
                         should be set if the function is executed on relation departed
                         event to avoid including data from the departed relation.

        Returns:
            list: List of certificates.
        """
        certificates = []
        relations = self.model.relations[self.relationship_name]
        for relation in relations:
            if relation.id == departed_id:
                continue
            for unit in relation.units:
                if unit.app is self.charm.app:
                    # it is peer relation, skip
                    continue
                relation_data = _load_relation_data(relation.data[unit])
                if not self._relation_data_is_valid(relation_data):
                    continue
                parsed_certificates = self._parse_certificates_from_relation_data(relation_data)
                for certificate in parsed_certificates:
                    if certificate["common_name"] != common_name:
                        continue
                    certificates.append(certificate)

        return certificates

    def _on_relation_changed(self, event) -> None:
        """Handler triggerred on relation changed events.

        Args:
            event: Juju event

        Returns:
            None
        """
        relation_data = _load_relation_data(event.relation.data[event.unit])
        if not self._relation_data_is_valid(relation_data):
            logger.warning("Relation data did not pass JSON Schema validation - Deferring")
            event.defer()
            return

        certificates = self._parse_certificates_from_relation_data(relation_data)
        for certificate in certificates:
            self.on.certificate_available.emit(certificate_data=certificate)
