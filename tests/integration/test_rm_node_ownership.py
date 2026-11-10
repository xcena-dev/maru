# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient node ownership against a private RM on fake devices.

Uses the CMake test-only daemon (maru_rm_process_test_daemon), which redirects
/dev and /sys into a temporary fixture, so no DAX device is opened. Run with:

    MARU_RM_TEST_DAEMON=<build>/maru_rm_process_test_daemon \\
        pytest tests/integration/test_rm_node_ownership.py -m integration
"""

import os
import re
import subprocess
import time
from pathlib import Path

import pytest

from maru_shm import client as client_module
from maru_shm.client import (
    MaruShmClient,
    NodeHostConflictError,
    StaleNodeEpochError,
)

pytestmark = pytest.mark.integration

ALIGN = 2 << 20
SIZE = 16 << 20
LEASE_S = 2


@pytest.fixture
def rm_address(tmp_path):
    daemon = os.environ.get("MARU_RM_TEST_DAEMON")
    if not daemon or Path(daemon).name != "maru_rm_process_test_daemon":
        pytest.skip("set MARU_RM_TEST_DAEMON to the CMake test-only daemon")
    root = tmp_path
    (root / ".rm-test-fixture").touch()
    (root / "state").mkdir()
    (root / "sys/class/block").mkdir(parents=True)
    sysfs = root / "sys/bus/dax/devices/dax0.0"
    sysfs.mkdir(parents=True)
    (sysfs / "driver").symlink_to("/sys/bus/dax/drivers/device_dax")
    (sysfs / "size").write_text(str(SIZE))
    (sysfs / "align").write_text(str(ALIGN))
    (root / "sys/class/dax/dax0.0").mkdir(parents=True)
    (root / "sys/class/dax/dax0.0/size").write_text(str(SIZE))
    (root / "dev").mkdir()
    with (root / "dev/dax0.0").open("wb") as stream:
        stream.truncate(SIZE)
    log = root / "daemon.log"
    env = dict(
        os.environ, MARU_TEST_DAX_ROOT=str(root), MARU_TEST_NODE_LEASE=str(LEASE_S)
    )
    with log.open("wb") as out:
        proc = subprocess.Popen([daemon], env=env, stdout=out, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        while "ready — listening" not in log.read_text():
            assert proc.poll() is None, log.read_text()
            assert time.monotonic() < deadline, log.read_text()
            time.sleep(0.05)
        yield f"127.0.0.1:{int((root / 'port').read_text())}"
    finally:
        proc.terminate()
        proc.wait(timeout=30)


def _client(address, client_id=None):
    client = MaruShmClient(address=address)
    if client_id:
        client._client_id = client_id
    return client


def test_claim_takeover_and_release(rm_address):
    first = _client(rm_address)
    claim = first.node_claim("gb1", "e1")
    assert re.fullmatch(r"[0-9a-f]{32}", claim.rm_domain)
    assert (claim.lease_s, claim.regions) == (LEASE_S, [])
    region = first.node_alloc("gb1", "e1", ALIGN, "/dev/dax0.0")
    assert (region.dax_path, region.requested_size) == ("/dev/dax0.0", ALIGN)
    assert len(region.device_uuid) == 36
    assert first.get_access(region.handle).dax_path == "/dev/dax0.0"
    first.close()

    # The next run of the node, from another process on this host.
    second = _client(rm_address)
    again = second.node_claim("gb1", "e2")
    assert (again.rm_domain, again.regions) == (claim.rm_domain, [region])
    with pytest.raises(StaleNodeEpochError):
        second.node_renew("gb1", "e1")
    with pytest.raises(StaleNodeEpochError):
        second.node_free("gb1", "e1", region.handle)
    with pytest.raises(NodeHostConflictError):
        _client(rm_address, "other-host:1").node_claim("gb1", "e3")
    assert second.node_renew("gb1", "e2") == LEASE_S
    assert second.node_release("gb1", "e2") == 1
    with pytest.raises(StaleNodeEpochError):
        second.node_renew("gb1", "e2")
    assert second.stats()[0].free_size == SIZE - ALIGN
    second.close()


def test_lease_expiry_frees_and_forgets(rm_address):
    client = _client(rm_address)
    client.node_claim("gb1", "e1")
    client.node_alloc("gb1", "e1", ALIGN)
    assert client.stats()[0].free_size == SIZE - 2 * ALIGN
    deadline = time.monotonic() + 30
    while client.stats()[0].free_size != SIZE - ALIGN:
        assert time.monotonic() < deadline
        time.sleep(0.2)
    with pytest.raises(StaleNodeEpochError):
        client.node_renew("gb1", "e1")
    # Forgotten: any host may claim the node now.
    other = _client(rm_address, "other-host:1")
    assert other.node_claim("gb1", "e2").regions == []
    other.close()
    client.close()


def test_restarted_run_reusing_ids_is_not_answered_from_cache(rm_address, monkeypatch):
    # Two runs of the node with the same hostname:pid (PID 1 in a container)
    # whose request IDs start at the same value.
    monkeypatch.setattr(client_module, "_request_id_counter", 0)
    first = _client(rm_address, "h:7")
    first.node_claim("n1", "e1")
    region = first.node_alloc("n1", "e1", ALIGN)
    first.close()
    monkeypatch.setattr(client_module, "_request_id_counter", 0)
    second = _client(rm_address, "h:7")
    assert second.node_claim("n1", "e2").regions == [region]
    other = second.node_alloc("n1", "e2", ALIGN)
    assert other.handle.region_id != region.handle.region_id
    with pytest.raises(StaleNodeEpochError):
        _client(rm_address, "h:8").node_renew("n1", "e1")
    assert second.node_renew("n1", "e2") == LEASE_S
    second.close()
