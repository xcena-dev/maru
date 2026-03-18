"""E2E test for auth gRPC cold path on real marufs.

Requires:
  - marufs mounted at /mnt/marufs
  - test certs generated: ./tools/gen_test_certs.sh ./test_certs
  - test policy: ./test_certs/policy.yaml

Usage:
  pytest tests/e2e/test_auth_e2e.py -v
"""

import mmap as mmap_module
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

MOUNT_PATH = "/mnt/marufs"
CERT_DIR = Path(__file__).parent.parent.parent / "test_certs"
PYTHON = sys.executable


def _marufs_mounted() -> bool:
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[1] == MOUNT_PATH and parts[2] == "marufs":
                    return True
    except OSError:
        pass
    return False


def _certs_exist() -> bool:
    required = ["ca.cert", "server.cert", "server.key", "prefill.cert",
                "prefill.key", "decode.cert", "decode.key", "policy.yaml"]
    return all((CERT_DIR / f).exists() for f in required)


@pytest.fixture(scope="module", autouse=True)
def ensure_certs():
    if not _certs_exist():
        subprocess.run(
            ["bash", str(CERT_DIR / "gen_test_certs.sh"), str(CERT_DIR)],
            check=True, capture_output=True,
        )


@pytest.fixture()
def auth_server():
    """Start auth gRPC server in a separate process."""
    server_script = f"""
import os, signal, sys
sys.path.insert(0, "{Path(__file__).parent.parent.parent}")
from maru_server.allocation_manager import AllocationManager
from maru_server.auth.grpc_server import AuthGrpcServer

alloc_mgr = AllocationManager("{MOUNT_PATH}")
auth_server = AuthGrpcServer(
    allocation_manager=alloc_mgr,
    policy_path="{CERT_DIR / 'policy.yaml'}",
    server_cert_path="{CERT_DIR / 'server.cert'}",
    server_key_path="{CERT_DIR / 'server.key'}",
    ca_cert_path="{CERT_DIR / 'ca.cert'}",
    host="localhost", port=50051,
)
auth_server.start()
print(f"SERVER_PID={{os.getpid()}}", flush=True)
signal.pause()
"""
    proc = subprocess.Popen(
        [PYTHON, "-c", server_script],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )

    server_pid = None
    for line in proc.stdout:
        line = line.strip()
        if line.startswith("SERVER_PID="):
            server_pid = int(line.split("=")[1])
            break
    time.sleep(0.5)

    yield {"proc": proc, "pid": server_pid}

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _cleanup_region(region_id: int) -> None:
    from marufs.client import MarufsClient
    client = MarufsClient(MOUNT_PATH)
    try:
        region_file = client._find_region_file(region_id)
        os.unlink(os.path.join(MOUNT_PATH, region_file))
    except (FileNotFoundError, RuntimeError):
        pass


@pytest.mark.skipif(not _marufs_mounted(), reason="marufs not mounted at /mnt/marufs")
class TestAuthE2E:

    def test_full_auth_flow(self, auth_server):
        """Owner alloc → CHOWN → GrantReady → Reader access → mmap."""
        from maru_handler.auth_client import AuthClient
        from marufs.client import MarufsClient
        from marufs.ioctl import PERM_GRANT
        from maru_shm.types import MaruHandle

        region_id = None
        try:
            # 1. Owner AllocRequest
            owner_auth = AuthClient(
                "localhost:50051",
                str(CERT_DIR / "prefill.cert"),
                str(CERT_DIR / "prefill.key"),
                str(CERT_DIR / "ca.cert"),
            )
            resp = owner_auth.request_alloc(
                size=2 * 1024 * 1024, node_id=1, pid=os.getpid(),
            )
            assert resp.success, resp.error
            region_id = resp.region_id

            # 2. Owner CHOWN + perm_grant(server, GRANT)
            owner_client = MarufsClient(MOUNT_PATH)
            region_file = owner_client._find_region_file(region_id)
            fd = owner_client._open_region(region_file, readonly=False)
            owner_client.chown(fd)
            owner_client.perm_grant(
                fd, resp.server_node_id, resp.server_pid, PERM_GRANT,
            )

            # 3. GrantReady
            grant_resp = owner_auth.notify_grant_ready(region_id)
            assert grant_resp.success, grant_resp.error

            # 4. Reader AccessRequest
            reader_auth = AuthClient(
                "localhost:50051",
                str(CERT_DIR / "decode.cert"),
                str(CERT_DIR / "decode.key"),
                str(CERT_DIR / "ca.cert"),
            )
            access_resp = reader_auth.request_access(
                region_id=region_id, node_id=1, pid=os.getpid(),
            )
            assert access_resp.success, access_resp.error
            assert access_resp.granted_perms > 0

            # 5. Reader mmap
            reader_client = MarufsClient(MOUNT_PATH)
            handle = MaruHandle(
                region_id=region_id, offset=0,
                length=2 * 1024 * 1024, auth_token=0,
            )
            mm = reader_client.mmap(
                handle, mmap_module.PROT_READ | mmap_module.PROT_WRITE,
            )
            assert len(mm) == 2 * 1024 * 1024

            owner_auth.close()
            reader_auth.close()

        finally:
            if region_id is not None:
                _cleanup_region(region_id)

    def test_unknown_role_rejected(self, auth_server):
        """Cert with unknown role should fail AllocRequest."""
        import grpc
        from maru_common.proto import auth_pb2, auth_pb2_grpc

        # Generate unknown role cert
        unknown_key = CERT_DIR / "unknown.key"
        unknown_cert = CERT_DIR / "unknown.cert"
        subprocess.run([
            "openssl", "genpkey", "-algorithm", "EC",
            "-pkeyopt", "ec_paramgen_curve:P-256", "-out", str(unknown_key),
        ], check=True, capture_output=True)
        csr = CERT_DIR / "unknown.csr"
        subprocess.run([
            "openssl", "req", "-new", "-key", str(unknown_key),
            "-out", str(csr), "-subj", "/CN=unknown",
        ], check=True, capture_output=True)
        subprocess.run([
            "openssl", "x509", "-req", "-in", str(csr),
            "-CA", str(CERT_DIR / "ca.cert"),
            "-CAkey", str(CERT_DIR / "ca.key"),
            "-CAcreateserial", "-out", str(unknown_cert),
            "-days", "1", "-extfile", "/dev/stdin",
        ], input=b"subjectAltName=URI:spiffe://maru.cluster/role/hacker\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth,clientAuth",
            check=True, capture_output=True)

        ca = (CERT_DIR / "ca.cert").read_bytes()
        credentials = grpc.ssl_channel_credentials(
            root_certificates=ca,
            private_key=unknown_key.read_bytes(),
            certificate_chain=unknown_cert.read_bytes(),
        )
        with grpc.secure_channel("localhost:50051", credentials) as ch:
            stub = auth_pb2_grpc.AuthServiceStub(ch)
            resp = stub.AllocRequest(
                auth_pb2.AllocRequestMsg(size=1024, node_id=99, pid=9999)
            )
        assert not resp.success
        assert "Unknown role" in resp.error

        # Cleanup temp certs
        for f in [unknown_key, unknown_cert, csr]:
            f.unlink(missing_ok=True)

    def test_no_auth_mmap_denied(self, auth_server):
        """Process without perm_grant should fail mmap."""
        from maru_handler.auth_client import AuthClient
        from marufs.client import MarufsClient
        from marufs.ioctl import PERM_GRANT
        from maru_shm.types import MaruHandle

        region_id = None
        try:
            # Owner creates region
            owner_auth = AuthClient(
                "localhost:50051",
                str(CERT_DIR / "prefill.cert"),
                str(CERT_DIR / "prefill.key"),
                str(CERT_DIR / "ca.cert"),
            )
            resp = owner_auth.request_alloc(
                size=2 * 1024 * 1024, node_id=1, pid=os.getpid(),
            )
            assert resp.success
            region_id = resp.region_id

            owner_client = MarufsClient(MOUNT_PATH)
            region_file = owner_client._find_region_file(region_id)
            fd = owner_client._open_region(region_file, readonly=False)
            owner_client.chown(fd)
            owner_client.perm_grant(
                fd, resp.server_node_id, resp.server_pid, PERM_GRANT,
            )
            owner_auth.notify_grant_ready(region_id)

            # Rogue client: separate process, NO AccessRequest, try mmap directly
            rogue_script = f"""
import sys, mmap as mmap_module
sys.path.insert(0, "{Path(__file__).parent.parent.parent}")
from marufs.client import MarufsClient
from maru_shm.types import MaruHandle
client = MarufsClient("{MOUNT_PATH}")
handle = MaruHandle(region_id={region_id}, offset=0, length={2*1024*1024}, auth_token=0)
try:
    client.mmap(handle, mmap_module.PROT_READ | mmap_module.PROT_WRITE)
    sys.exit(0)  # should not reach here
except (PermissionError, OSError):
    sys.exit(42)  # expected
"""
            result = subprocess.run(
                [PYTHON, "-c", rogue_script],
                capture_output=True, timeout=10,
            )
            assert result.returncode == 42, (
                f"Rogue mmap should be denied, got exit={result.returncode}\n"
                f"stderr: {result.stderr.decode()}"
            )

            owner_auth.close()

        finally:
            if region_id is not None:
                _cleanup_region(region_id)

    def test_no_cert_connection_rejected(self, auth_server):
        """Connection without client cert should be rejected."""
        import grpc
        from maru_common.proto import auth_pb2, auth_pb2_grpc

        # Connect with server CA but no client cert (no mTLS)
        ca = (CERT_DIR / "ca.cert").read_bytes()
        credentials = grpc.ssl_channel_credentials(root_certificates=ca)
        with grpc.secure_channel("localhost:50051", credentials) as ch:
            stub = auth_pb2_grpc.AuthServiceStub(ch)
            with pytest.raises(grpc.RpcError) as exc_info:
                stub.AllocRequest(
                    auth_pb2.AllocRequestMsg(size=1024, node_id=1, pid=1)
                )
            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE

    def test_multiple_readers(self, auth_server):
        """Multiple readers should each get independent access grants."""
        from maru_handler.auth_client import AuthClient
        from marufs.client import MarufsClient
        from marufs.ioctl import PERM_GRANT

        region_id = None
        try:
            # Owner creates region
            owner_auth = AuthClient(
                "localhost:50051",
                str(CERT_DIR / "prefill.cert"),
                str(CERT_DIR / "prefill.key"),
                str(CERT_DIR / "ca.cert"),
            )
            resp = owner_auth.request_alloc(
                size=2 * 1024 * 1024, node_id=1, pid=os.getpid(),
            )
            assert resp.success
            region_id = resp.region_id

            owner_client = MarufsClient(MOUNT_PATH)
            region_file = owner_client._find_region_file(region_id)
            fd = owner_client._open_region(region_file, readonly=False)
            owner_client.chown(fd)
            owner_client.perm_grant(
                fd, resp.server_node_id, resp.server_pid, PERM_GRANT,
            )
            owner_auth.notify_grant_ready(region_id)

            # Two readers request access with different pids (simulated)
            reader1 = AuthClient(
                "localhost:50051",
                str(CERT_DIR / "decode.cert"),
                str(CERT_DIR / "decode.key"),
                str(CERT_DIR / "ca.cert"),
            )
            reader2 = AuthClient(
                "localhost:50051",
                str(CERT_DIR / "decode.cert"),
                str(CERT_DIR / "decode.key"),
                str(CERT_DIR / "ca.cert"),
            )

            resp1 = reader1.request_access(region_id=region_id, node_id=1, pid=10001)
            resp2 = reader2.request_access(region_id=region_id, node_id=1, pid=10002)

            assert resp1.success, resp1.error
            assert resp2.success, resp2.error
            assert resp1.granted_perms == resp2.granted_perms

            owner_auth.close()
            reader1.close()
            reader2.close()

        finally:
            if region_id is not None:
                _cleanup_region(region_id)
