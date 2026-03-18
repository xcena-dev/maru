"""SPIFFE ID extraction and parsing from X.509 certificates.

SPIFFE ID format:
    spiffe://maru.cluster/node-{node_id}/role/{role}/instance/{pid}
    spiffe://maru.cluster/role/{role}

The role is extracted from the path segment after "role/".
"""

import logging
from urllib.parse import urlparse

from cryptography import x509
from cryptography.x509.oid import ExtensionOID

logger = logging.getLogger(__name__)


def extract_spiffe_id_from_cert(cert: x509.Certificate) -> str | None:
    """Extract SPIFFE ID (URI SAN starting with spiffe://) from an X.509 cert."""
    try:
        san_ext = cert.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME
        )
        uris = san_ext.value.get_values_for_type(
            x509.UniformResourceIdentifier
        )
        for uri in uris:
            if uri.startswith("spiffe://"):
                return uri
    except x509.ExtensionNotFound:
        pass
    return None


def extract_spiffe_id(auth_context: dict) -> str | None:
    """Extract SPIFFE ID from gRPC auth context (mTLS peer cert).

    Args:
        auth_context: from grpc.ServicerContext.auth_context()

    Returns:
        SPIFFE ID string or None if not found.
    """
    peer_cert_chain = auth_context.get("x509_pem_cert")
    if not peer_cert_chain:
        return None

    cert_pem = peer_cert_chain[0]
    if isinstance(cert_pem, str):
        cert_pem = cert_pem.encode()
    cert = x509.load_pem_x509_certificate(cert_pem)
    return extract_spiffe_id_from_cert(cert)


def parse_role_from_spiffe_id(spiffe_id: str) -> str | None:
    """Parse role from SPIFFE ID path.

    Supports formats:
        spiffe://maru.cluster/node-1/role/prefill/instance/4521
        spiffe://maru.cluster/role/server

    Returns:
        Role string (e.g., "prefill", "decode", "server") or None.
    """
    parsed = urlparse(spiffe_id)
    parts = parsed.path.strip("/").split("/")
    for i, part in enumerate(parts):
        if part == "role" and i + 1 < len(parts):
            return parts[i + 1]
    return None
