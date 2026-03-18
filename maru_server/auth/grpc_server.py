"""gRPC mTLS authentication server for Maru (cold path).

Handles ALLOC_REQUEST, ACCESS_REQUEST, GRANT_READY over mTLS.
Client identity is extracted from the peer cert SAN (SPIFFE ID).
Hot path (ZMQ) remains unchanged.
"""

import logging
import threading
from concurrent import futures
from pathlib import Path

import grpc

from maru_common.proto import auth_pb2, auth_pb2_grpc
from maru_server.auth.policy import PolicyTable
from maru_server.auth.region_state import (
    RegionState,
    RegionStateManager,
)
from maru_server.auth.spiffe import extract_spiffe_id, parse_role_from_spiffe_id

logger = logging.getLogger(__name__)


class AuthServicer(auth_pb2_grpc.AuthServiceServicer):
    """gRPC servicer implementing the auth cold path."""

    def __init__(
        self,
        allocation_manager,
        policy: PolicyTable,
        state_manager: RegionStateManager,
    ):
        import os

        self._alloc_mgr = allocation_manager
        self._client = allocation_manager._client
        self._policy = policy
        self._state = state_manager
        self._region_ids: set[int] = set()  # tracked region_ids
        self._server_pid = os.getpid()
        try:
            self._server_node_id = self._client.get_node_id()
        except (RuntimeError, AttributeError):
            self._server_node_id = 0
        self._mount_path = getattr(self._client, '_mount_path', '') or ''
        logger.info(
            "AuthServicer: server_node_id=%d server_pid=%d mount_path=%s",
            self._server_node_id, self._server_pid, self._mount_path,
        )

    def _get_caller_identity(self, context: grpc.ServicerContext):
        """Extract SPIFFE ID and role from mTLS peer cert."""
        spiffe_id = extract_spiffe_id(context.auth_context())
        if not spiffe_id:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "No SPIFFE ID in peer cert")
        role = parse_role_from_spiffe_id(spiffe_id)
        if not role:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                f"Cannot parse role from SPIFFE ID: {spiffe_id}",
            )
        return spiffe_id, role

    def AllocRequest(self, request, context):
        spiffe_id, role = self._get_caller_identity(context)

        perms = self._policy.lookup(role)
        if perms is None:
            return auth_pb2.AllocResponseMsg(
                success=False, error=f"Unknown role: {role}"
            )

        logger.info(
            "AllocRequest: size=%d role=%s node=%d pid=%d",
            request.size, role, request.node_id, request.pid,
        )

        try:
            # Create region via AllocationManager (registers in ZMQ path too)
            instance_id = spiffe_id  # use SPIFFE ID as instance identifier
            handle = self._alloc_mgr.allocate(instance_id, request.size)
            if handle is None:
                raise RuntimeError("AllocationManager.allocate() returned None")
            region_id = handle.region_id

            # Create region state (keyed by region_id)
            self._state.create(region_id, spiffe_id)

            # Set no default permissions (least privilege)
            fd = self._client.get_fd(region_id)
            if fd is None:
                raise RuntimeError(f"get_fd returned None for region_id={region_id}")
            self._client.perm_set_default(fd, 0)
            # Grant ADMIN to owner so they can CHOWN
            self._client.perm_grant(fd, request.node_id, request.pid, 0x0008)
            logger.info(
                "AllocRequest: perm_set_default(0) + perm_grant(ADMIN) done "
                "fd=%d node=%d pid=%d", fd, request.node_id, request.pid,
            )

            self._region_ids.add(region_id)

            # Transition to CHOWN_PENDING
            self._state.transition_to_chown_pending(region_id)

            return auth_pb2.AllocResponseMsg(
                success=True,
                region_id=region_id,
                server_node_id=self._server_node_id,
                server_pid=self._server_pid,
                mount_path=self._mount_path,
            )
        except Exception as e:
            logger.error("AllocRequest failed: %s", e)
            self._state.remove(region_id if 'region_id' in dir() else 0)
            return auth_pb2.AllocResponseMsg(success=False, error=str(e))

    def AccessRequest(self, request, context):
        spiffe_id, role = self._get_caller_identity(context)

        perms = self._policy.lookup(role)
        if perms is None:
            return auth_pb2.AccessResponseMsg(
                success=False, error=f"Unknown role: {role}"
            )

        logger.info(
            "AccessRequest: region_id=%d role=%s node=%d pid=%d perms=0x%04x",
            request.region_id, role, request.node_id, request.pid, perms,
        )

        try:
            state = self._state.get_state(request.region_id)
        except KeyError:
            return auth_pb2.AccessResponseMsg(
                success=False, error=f"Region {request.region_id} not found"
            )

        if state == RegionState.CHOWN_PENDING:
            # Queue and wait
            queued = self._state.queue_access_request(
                request.region_id, request.node_id, request.pid, role
            )
            # Block until GRANT_READY processes this request
            if not queued.event.wait(timeout=30.0):
                return auth_pb2.AccessResponseMsg(
                    success=False, error="Timeout waiting for GRANT_READY"
                )
            if queued.error:
                return auth_pb2.AccessResponseMsg(
                    success=False, error=queued.error
                )
            return auth_pb2.AccessResponseMsg(
                success=True, granted_perms=queued.granted_perms
            )

        if state != RegionState.ACTIVE:
            return auth_pb2.AccessResponseMsg(
                success=False,
                error=f"Region {request.region_id} in state {state.value}",
            )

        # Grant permissions
        try:
            self._grant_access(request.region_id, request.node_id, request.pid, perms)
            return auth_pb2.AccessResponseMsg(success=True, granted_perms=perms)
        except Exception as e:
            logger.error(
                "AccessRequest perm_grant failed: %s "
                "(region_id=%d, target_node=%d, target_pid=%d, perms=0x%04x, "
                "server_node=%d, server_pid=%d)",
                e, request.region_id, request.node_id, request.pid, perms,
                self._server_node_id, self._server_pid,
            )
            return auth_pb2.AccessResponseMsg(success=False, error=str(e))

    def GrantReady(self, request, context):
        spiffe_id, role = self._get_caller_identity(context)

        logger.info(
            "GrantReady: region_id=%d from=%s", request.region_id, spiffe_id
        )

        try:
            queued_requests = self._state.grant_ready(
                request.region_id, spiffe_id
            )
        except PermissionError as e:
            return auth_pb2.GrantReadyResponseMsg(success=False, error=str(e))
        except (KeyError, ValueError) as e:
            return auth_pb2.GrantReadyResponseMsg(success=False, error=str(e))

        # Process queued ACCESS_REQUESTs
        for req in queued_requests:
            perms = self._policy.lookup(req.role)
            if perms is None:
                req.error = f"Unknown role: {req.role}"
                req.event.set()
                continue
            try:
                self._grant_access(
                    req.region_id, req.node_id, req.pid, perms
                )
                req.granted_perms = perms
            except Exception as e:
                req.error = str(e)
            req.event.set()

        return auth_pb2.GrantReadyResponseMsg(success=True)

    def _grant_access(
        self, region_id: int, node_id: int, pid: int, perms: int
    ) -> None:
        """Call perm_grant via MarufsClient for a region."""
        fd = self._client.get_fd(region_id)
        if fd is None:
            raise ValueError(f"No fd found for region_id={region_id}")
        logger.debug(
            "_grant_access: region_id=%d fd=%d node=%d pid=%d perms=0x%04x",
            region_id, fd, node_id, pid, perms,
        )
        self._client.perm_grant(fd, node_id, pid, perms)


class AuthGrpcServer:
    """Manages the gRPC mTLS server lifecycle."""

    def __init__(
        self,
        allocation_manager,
        policy_path: str | Path,
        server_cert_path: str | Path,
        server_key_path: str | Path,
        ca_cert_path: str | Path,
        host: str = "0.0.0.0",
        port: int = 50051,
        max_workers: int = 4,
    ):
        self._host = host
        self._port = port

        # Load certs
        server_cert = Path(server_cert_path).read_bytes()
        server_key = Path(server_key_path).read_bytes()
        ca_cert = Path(ca_cert_path).read_bytes()

        # Auth components
        policy = PolicyTable(policy_path)
        state_manager = RegionStateManager()
        servicer = AuthServicer(allocation_manager, policy, state_manager)

        # gRPC server with mTLS
        credentials = grpc.ssl_server_credentials(
            [(server_key, server_cert)],
            root_certificates=ca_cert,
            require_client_auth=True,
        )
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=max_workers)
        )
        auth_pb2_grpc.add_AuthServiceServicer_to_server(servicer, self._server)
        self._server.add_secure_port(f"{host}:{port}", credentials)

        self.policy = policy
        self.state_manager = state_manager

    def start(self) -> None:
        self._server.start()
        logger.info("Auth gRPC server listening on %s:%d (mTLS)", self._host, self._port)

    def stop(self, grace: float = 5.0) -> None:
        self._server.stop(grace)
        logger.info("Auth gRPC server stopped")

    def wait_for_termination(self) -> None:
        self._server.wait_for_termination()
