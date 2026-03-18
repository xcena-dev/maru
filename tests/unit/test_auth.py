"""Unit + integration tests for auth gRPC cold path.

Tests:
1. PolicyTable loading and lookup
2. SPIFFE ID extraction and role parsing
3. RegionStateManager state transitions and request queuing
4. gRPC mTLS server E2E (AllocRequest, AccessRequest, GrantReady)
"""

import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc
import pytest
import yaml

from maru_server.auth.policy import PolicyTable, PERM_FLAGS
from maru_server.auth.region_state import RegionState, RegionStateManager
from maru_server.auth.spiffe import (
    extract_spiffe_id_from_cert,
    parse_role_from_spiffe_id,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def policy_file(tmp_path):
    policy = {
        "roles": {
            "prefill": {"perms": ["READ", "WRITE", "DELETE", "ADMIN", "IOCTL"]},
            "decode": {"perms": ["READ"]},
            "server": {"perms": ["GRANT"]},
            "admin": {"perms": ["READ", "WRITE", "DELETE", "ADMIN", "IOCTL", "GRANT"]},
        }
    }
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.dump(policy))
    return path


@pytest.fixture
def certs(tmp_path):
    """Generate test certs using gen_test_certs.sh."""
    script = Path(__file__).parent.parent.parent / "test_certs" / "gen_test_certs.sh"
    cert_dir = tmp_path / "certs"
    subprocess.run(
        ["bash", str(script), str(cert_dir)],
        check=True,
        capture_output=True,
    )
    return {
        "ca": cert_dir / "ca.cert",
        "server_cert": cert_dir / "server.cert",
        "server_key": cert_dir / "server.key",
        "prefill_cert": cert_dir / "prefill.cert",
        "prefill_key": cert_dir / "prefill.key",
        "decode_cert": cert_dir / "decode.cert",
        "decode_key": cert_dir / "decode.key",
    }


# ============================================================================
# PolicyTable tests
# ============================================================================

class TestPolicyTable:
    def test_load_roles(self, policy_file):
        p = PolicyTable(policy_file)
        assert set(p.roles) == {"prefill", "decode", "server", "admin"}

    def test_prefill_perms(self, policy_file):
        p = PolicyTable(policy_file)
        expected = (
            PERM_FLAGS["READ"]
            | PERM_FLAGS["WRITE"]
            | PERM_FLAGS["DELETE"]
            | PERM_FLAGS["ADMIN"]
            | PERM_FLAGS["IOCTL"]
        )
        assert p.lookup("prefill") == expected

    def test_decode_perms(self, policy_file):
        p = PolicyTable(policy_file)
        assert p.lookup("decode") == PERM_FLAGS["READ"]

    def test_unknown_role(self, policy_file):
        p = PolicyTable(policy_file)
        assert p.lookup("unknown") is None

    def test_has_role(self, policy_file):
        p = PolicyTable(policy_file)
        assert p.has_role("prefill")
        assert not p.has_role("unknown")


# ============================================================================
# SPIFFE tests
# ============================================================================

class TestSpiffe:
    def test_parse_role_prefill(self):
        spiffe_id = "spiffe://maru.cluster/node-1/role/prefill/instance/4521"
        assert parse_role_from_spiffe_id(spiffe_id) == "prefill"

    def test_parse_role_server(self):
        assert parse_role_from_spiffe_id("spiffe://maru.cluster/role/server") == "server"

    def test_parse_role_decode(self):
        spiffe_id = "spiffe://maru.cluster/node-2/role/decode/instance/8832"
        assert parse_role_from_spiffe_id(spiffe_id) == "decode"

    def test_parse_no_role(self):
        assert parse_role_from_spiffe_id("spiffe://maru.cluster/no-role") is None

    def test_extract_from_cert(self, certs):
        from cryptography import x509

        cert_pem = certs["prefill_cert"].read_bytes()
        cert = x509.load_pem_x509_certificate(cert_pem)
        spiffe_id = extract_spiffe_id_from_cert(cert)
        assert spiffe_id == "spiffe://maru.cluster/node-1/role/prefill"


# ============================================================================
# RegionStateManager tests
# ============================================================================

class TestRegionStateManager:
    def test_lifecycle(self):
        sm = RegionStateManager()
        sm.create(1, "spiffe://maru.cluster/role/prefill")
        assert sm.get_state(1) == RegionState.CREATING

        sm.transition_to_chown_pending(1)
        assert sm.get_state(1) == RegionState.CHOWN_PENDING

        queued = sm.grant_ready(1, "spiffe://maru.cluster/role/prefill")
        assert sm.get_state(1) == RegionState.ACTIVE
        assert len(queued) == 0

    def test_grant_ready_wrong_owner(self):
        sm = RegionStateManager()
        sm.create(1, "spiffe://owner")
        sm.transition_to_chown_pending(1)
        with pytest.raises(PermissionError):
            sm.grant_ready(1, "spiffe://not-owner")

    def test_queue_access_request(self):
        sm = RegionStateManager()
        sm.create(1, "spiffe://owner")
        sm.transition_to_chown_pending(1)

        req = sm.queue_access_request(1, node_id=2, pid=8832, role="decode")
        assert not req.event.is_set()

        queued = sm.grant_ready(1, "spiffe://owner")
        assert len(queued) == 1
        assert queued[0].pid == 8832

    def test_duplicate_create_fails(self):
        sm = RegionStateManager()
        sm.create(1, "spiffe://owner")
        with pytest.raises(ValueError):
            sm.create(1, "spiffe://other")

    def test_remove(self):
        sm = RegionStateManager()
        sm.create(1, "spiffe://owner")
        sm.remove(1)
        with pytest.raises(KeyError):
            sm.get_state(1)


# ============================================================================
# gRPC E2E test
# ============================================================================

class TestAuthGrpcE2E:
    """End-to-end test: start gRPC server with mTLS, connect as client."""

    def _make_channel(self, certs, cert_name, port):
        """Create an mTLS gRPC channel using the specified client cert."""
        ca = certs["ca"].read_bytes()
        client_cert = certs[f"{cert_name}_cert"].read_bytes()
        client_key = certs[f"{cert_name}_key"].read_bytes()
        credentials = grpc.ssl_channel_credentials(
            root_certificates=ca,
            private_key=client_key,
            certificate_chain=client_cert,
        )
        return grpc.secure_channel(f"localhost:{port}", credentials)

    def test_full_auth_flow(self, certs, policy_file):
        """Test: prefill allocs → CHOWN → GRANT_READY → decode gets READ."""
        from maru_server.auth.grpc_server import AuthGrpcServer
        from maru_common.proto import auth_pb2, auth_pb2_grpc

        # Mock AllocationManager + MarufsClient
        mock_client = MagicMock()
        mock_client._region_names = {0: "llama3_kv"}
        mock_client.get_fd.return_value = 42
        mock_client.node_id = 1
        mock_client._mount_path = "/mnt/marufs"

        mock_alloc_mgr = MagicMock()
        mock_handle = MagicMock()
        mock_handle.region_id = 0
        mock_alloc_mgr.allocate.return_value = mock_handle
        mock_alloc_mgr._client = mock_client

        port = 50199  # test port

        server = AuthGrpcServer(
            allocation_manager=mock_alloc_mgr,
            policy_path=policy_file,
            server_cert_path=certs["server_cert"],
            server_key_path=certs["server_key"],
            ca_cert_path=certs["ca"],
            host="localhost",
            port=port,
        )
        server.start()

        try:
            # --- Step 1: Prefill AllocRequest ---
            with self._make_channel(certs, "prefill", port) as ch:
                stub = auth_pb2_grpc.AuthServiceStub(ch)
                resp = stub.AllocRequest(
                    auth_pb2.AllocRequestMsg(
                        size=4 * 1024**3,
                        node_id=1, pid=4521,
                    )
                )
            assert resp.success, resp.error
            region_id = resp.region_id

            # Verify perm_set_default(0) and perm_grant(ADMIN) were called
            mock_client.perm_set_default.assert_called_once_with(42, 0)
            mock_client.perm_grant.assert_called_once_with(42, 1, 4521, 0x0008)

            # --- Step 2: Decode AccessRequest (should queue, region is CHOWN_PENDING) ---
            decode_result = {}

            def decode_access():
                with self._make_channel(certs, "decode", port) as ch:
                    stub = auth_pb2_grpc.AuthServiceStub(ch)
                    resp = stub.AccessRequest(
                        auth_pb2.AccessRequestMsg(
                            region_id=region_id, node_id=2, pid=8832,
                        )
                    )
                    decode_result["resp"] = resp

            t = threading.Thread(target=decode_access)
            t.start()
            time.sleep(0.5)  # let it queue

            # --- Step 3: Prefill GrantReady ---
            mock_client.perm_grant.reset_mock()
            with self._make_channel(certs, "prefill", port) as ch:
                stub = auth_pb2_grpc.AuthServiceStub(ch)
                resp = stub.GrantReady(
                    auth_pb2.GrantReadyMsg(region_id=region_id)
                )
            assert resp.success, resp.error

            t.join(timeout=5.0)
            assert "resp" in decode_result
            assert decode_result["resp"].success
            assert decode_result["resp"].granted_perms == PERM_FLAGS["READ"]

            # Verify perm_grant(READ) called for decode
            mock_client.perm_grant.assert_called_once_with(42, 2, 8832, PERM_FLAGS["READ"])

        finally:
            server.stop(grace=1.0)

    def test_unknown_role_rejected(self, certs, policy_file, tmp_path):
        """Test: cert with unknown role gets rejected."""
        from maru_server.auth.grpc_server import AuthGrpcServer
        from maru_common.proto import auth_pb2, auth_pb2_grpc

        # Generate a cert with unknown role
        unknown_key = tmp_path / "unknown.key"
        unknown_cert = tmp_path / "unknown.cert"
        unknown_csr = tmp_path / "unknown.csr"
        subprocess.run([
            "openssl", "genpkey", "-algorithm", "EC",
            "-pkeyopt", "ec_paramgen_curve:P-256", "-out", str(unknown_key),
        ], check=True, capture_output=True)
        subprocess.run([
            "openssl", "req", "-new", "-key", str(unknown_key),
            "-out", str(unknown_csr), "-subj", "/CN=unknown-0",
        ], check=True, capture_output=True)
        subprocess.run([
            "openssl", "x509", "-req", "-in", str(unknown_csr),
            "-CA", str(certs["ca"]), "-CAkey", str(tmp_path / "certs" / "ca.key"),
            "-CAcreateserial", "-out", str(unknown_cert),
            "-days", "1",
            "-extfile", "/dev/stdin",
        ], input=b"subjectAltName=URI:spiffe://maru.cluster/role/hacker\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth,clientAuth",
            check=True, capture_output=True)

        mock_client = MagicMock()
        mock_client._mount_path = "/mnt/marufs"
        mock_alloc_mgr = MagicMock()
        mock_alloc_mgr._client = mock_client
        port = 50198

        server = AuthGrpcServer(
            allocation_manager=mock_alloc_mgr,
            policy_path=policy_file,
            server_cert_path=certs["server_cert"],
            server_key_path=certs["server_key"],
            ca_cert_path=certs["ca"],
            host="localhost",
            port=port,
        )
        server.start()

        try:
            ca = certs["ca"].read_bytes()
            credentials = grpc.ssl_channel_credentials(
                root_certificates=ca,
                private_key=unknown_key.read_bytes(),
                certificate_chain=unknown_cert.read_bytes(),
            )
            with grpc.secure_channel(f"localhost:{port}", credentials) as ch:
                stub = auth_pb2_grpc.AuthServiceStub(ch)
                resp = stub.AllocRequest(
                    auth_pb2.AllocRequestMsg(
                        size=1024, node_id=99, pid=9999,
                    )
                )
            assert not resp.success
            assert "Unknown role" in resp.error
        finally:
            server.stop(grace=1.0)
