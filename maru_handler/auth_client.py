"""gRPC mTLS auth client for Maru cold path.

Connects to the Maru Server's auth gRPC endpoint to:
1. AllocRequest  — request region allocation (owner)
2. AccessRequest — request access to existing region (reader)
3. GrantReady    — confirm CHOWN complete + GRANT re-granted to server
"""

import logging
from pathlib import Path

import grpc

from maru_common.proto import auth_pb2, auth_pb2_grpc

logger = logging.getLogger(__name__)


class AuthClient:
    """mTLS gRPC client for Maru auth cold path."""

    def __init__(
        self,
        server_addr: str,
        client_cert_path: str | Path,
        client_key_path: str | Path,
        ca_cert_path: str | Path,
    ):
        client_cert = Path(client_cert_path).read_bytes()
        client_key = Path(client_key_path).read_bytes()
        ca_cert = Path(ca_cert_path).read_bytes()

        credentials = grpc.ssl_channel_credentials(
            root_certificates=ca_cert,
            private_key=client_key,
            certificate_chain=client_cert,
        )
        self._channel = grpc.secure_channel(server_addr, credentials)
        self._stub = auth_pb2_grpc.AuthServiceStub(self._channel)
        self._server_addr = server_addr
        logger.info("AuthClient connected to %s (mTLS)", server_addr)

    def request_alloc(
        self, size: int, node_id: int, pid: int
    ) -> auth_pb2.AllocResponseMsg:
        """Request region allocation (owner)."""
        resp = self._stub.AllocRequest(
            auth_pb2.AllocRequestMsg(
                size=size, node_id=node_id, pid=pid,
            )
        )
        if resp.success:
            logger.info("AllocRequest OK: region_id=%d", resp.region_id)
        else:
            logger.error("AllocRequest failed: %s", resp.error)
        return resp

    def request_access(
        self, region_id: int, node_id: int, pid: int
    ) -> auth_pb2.AccessResponseMsg:
        """Request access to existing region (reader)."""
        resp = self._stub.AccessRequest(
            auth_pb2.AccessRequestMsg(
                region_id=region_id, node_id=node_id, pid=pid,
            )
        )
        if resp.success:
            logger.info(
                "AccessRequest OK: region_id=%d perms=0x%04x",
                region_id, resp.granted_perms,
            )
        else:
            logger.error("AccessRequest failed: %s", resp.error)
        return resp

    def notify_grant_ready(self, region_id: int) -> auth_pb2.GrantReadyResponseMsg:
        """Confirm CHOWN complete + GRANT re-granted to server (owner)."""
        resp = self._stub.GrantReady(
            auth_pb2.GrantReadyMsg(region_id=region_id)
        )
        if resp.success:
            logger.info("GrantReady OK: region_id=%d", region_id)
        else:
            logger.error("GrantReady failed: %s", resp.error)
        return resp

    def close(self) -> None:
        self._channel.close()
        logger.info("AuthClient closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
