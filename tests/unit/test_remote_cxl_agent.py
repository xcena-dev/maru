# SPDX-License-Identifier: Apache-2.0
"""Remote CXL agent: the map / unmap / release service, its TCP front end, the
Handler-side client, the command line and the device mapper."""

import ctypes
import gc
import os
import threading
import time

import pytest

from maru_remote.region_map import (
    KeyFileError,
    RegionMapClient,
    RegionMapError,
    RegionMapUnavailable,
    load_key,
    prove,
    read_handle,
    write_key,
)
from maru_remote.transport import NixlTransport
from maru_remote_cxl_agent import cli
from maru_remote_cxl_agent.mapper import DaxRegionMapper
from maru_remote_cxl_agent.server import ConnectionAuth, handle_request
from maru_remote_cxl_agent.service import MapDenied, NotMapped, RegionMapService
from maru_shm.types import MaruHandle
from tests.unit.remote_fakes import (
    FakeCxl,
    FakeDeviceMapper,
    FakeMemoryNode,
    FakeNixlAgent,
    key_file,
    reset_fake_agents,
    shared_key,
)


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


class Node:
    """A region map service with fake RM, device and NIXL."""

    def __init__(self):
        self.cxl = FakeCxl()
        self.agent = FakeNixlAgent("mapper")
        self.transport = NixlTransport("mapper", agent=self.agent)
        self.mapper = FakeDeviceMapper(self.cxl)
        self.service = RegionMapService(
            self.cxl,
            self.mapper,
            self.transport,
            node_id="mem1",
            epoch="e1",
            boot_id="b1",
        )

    def region(self, size=4096):
        return self.cxl.reserve("owner", size).to_dict()


@pytest.fixture
def node():
    return Node()


def wait_for(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        time.sleep(0.01)


# ------------------------------------------------------------------ service


def test_the_first_map_checks_the_handle_and_later_maps_share_it(node):
    handle = node.region()
    first = node.service.map(1, handle)
    second = node.service.map(2, handle)
    assert node.cxl.accesses == [handle["region_id"]]  # one GET_ACCESS
    assert first == second and first["addr"] == node.mapper.base + handle["offset"]
    assert node.service.holders() == {handle["region_id"]: {1, 2}}
    assert node.agent.registered == [(first["addr"], handle["length"])]


def test_a_map_reply_names_only_that_region(node):
    h1, h2 = node.region(), node.region()
    reply = node.service.map(1, h1)
    node.service.map(1, h2)
    name, conn, ranges = reply["metadata"].decode().split("|")
    assert name == "mapper" and conn == "1"
    assert [tuple(map(int, r.split(":")[:2])) for r in ranges.split(";")] == [
        (reply["addr"], h1["length"])
    ]


def test_a_handle_that_does_not_match_is_denied(node):
    handle = node.region()
    node.service.map(1, handle)
    with pytest.raises(MapDenied):
        node.service.map(2, {**handle, "auth_token": handle["auth_token"] + 1})
    with pytest.raises(RuntimeError, match="GetAccess"):
        node.service.map(2, {**node.region(), "auth_token": 1})  # RM refuses


def test_the_last_unmap_deregisters_and_unmaps(node):
    handle = node.region()
    node.service.map(1, handle)
    node.service.map(2, handle)
    node.service.unmap(1, handle["region_id"])
    assert node.agent.registered and not node.mapper.unmapped
    node.service.unmap(2, handle["region_id"])
    assert node.agent.registered == [] and node.mapper.unmapped == [handle["offset"]]
    assert node.service.holders() == {}


def test_a_closed_connection_drops_its_holds(node):
    h1, h2 = node.region(), node.region()
    node.service.map(1, h1)
    node.service.map(1, h2)
    node.service.map(2, h2)
    node.service.drop(1)
    assert node.service.holders() == {h2["region_id"]: {2}}


def test_release_deregisters_whoever_holds_it_and_refuses_a_later_map(node):
    handle = node.region()
    node.service.map(1, handle)
    node.service.map(2, handle)  # a reader
    node.service.release(handle)
    assert node.agent.registered == [] and node.service.holders() == {}
    with pytest.raises(MapDenied, match="released"):
        node.service.map(2, handle)
    node.service.release(handle)  # a resent release succeeds


def test_releasing_a_region_that_is_not_mapped_succeeds_and_blocks_its_map(node):
    handle = node.region()
    node.service.release(handle)
    with pytest.raises(MapDenied, match="released"):
        node.service.map(1, handle)


def test_a_new_region_under_a_released_id_maps(node):
    handle = node.region()
    node.service.map(1, handle)
    node.service.release(handle)
    # A restarted resource manager hands out the id again with a new token.
    reused = {**handle, "auth_token": 9999}
    node.cxl.regions[handle["region_id"]] = MaruHandle(**reused)
    assert node.service.map(1, reused)["region_id"] == handle["region_id"]


def test_a_refused_map_under_a_released_id_keeps_the_release(node):
    """A map with another token the resource manager refuses leaves the
    record: the released handle stays refused."""
    handle = node.region()
    node.service.map(1, handle)
    node.service.release(handle)
    with pytest.raises(RuntimeError, match="GetAccess"):
        node.service.map(2, {**handle, "auth_token": handle["auth_token"] + 1})
    with pytest.raises(MapDenied, match="released"):
        node.service.map(2, handle)
    assert node.agent.registered == []


def test_a_release_with_another_handle_is_denied(node):
    handle = node.region()
    node.service.map(1, handle)
    with pytest.raises(MapDenied):
        node.service.release({**handle, "auth_token": handle["auth_token"] + 1})
    assert node.agent.registered


def test_a_failed_deregistration_keeps_the_mapping_and_fails_the_release(node):
    handle = node.region()
    node.service.map(1, handle)

    def refuse(registration):
        raise RuntimeError("NIC busy")

    node.transport.deregister = refuse
    with pytest.raises(RuntimeError, match="deregistering"):
        node.service.release(handle)
    assert node.service.holders() == {handle["region_id"]: {1}}
    assert node.mapper.unmapped == []


def test_concurrent_maps_of_one_region_check_it_once(node):
    handle = node.region()
    entered, go = threading.Event(), threading.Event()
    get_access = node.cxl.get_access

    def slow(h):
        entered.set()
        go.wait(5)
        return get_access(h)

    node.cxl.get_access = slow
    replies = []
    threads = [
        threading.Thread(target=lambda c=c: replies.append(node.service.map(c, handle)))
        for c in (1, 2, 3)
    ]
    for t in threads:
        t.start()
    entered.wait(5)
    go.set()
    for t in threads:
        t.join(5)
    assert len(replies) == 3 and len({r["addr"] for r in replies}) == 1
    assert node.cxl.accesses == [handle["region_id"]]
    assert node.service.holders() == {handle["region_id"]: {1, 2, 3}}


def test_a_failed_map_leaves_nothing_and_can_be_retried(node):
    handle = node.region()

    def refuse(*args, **kwargs):
        raise RuntimeError("NIXL refused")

    register = node.transport.register
    node.transport.register = refuse
    with pytest.raises(RuntimeError, match="refused"):
        node.service.map(1, handle)
    # No NIC key named the addresses, so they are not kept out of use.
    assert node.mapper.discarded == [handle["offset"]] and node.mapper.unmapped == []
    assert node.service.holders() == {}
    node.transport.register = register
    assert node.service.map(1, handle)["region_id"] == handle["region_id"]


def test_a_map_that_fails_after_registering_keeps_the_addresses_out_of_use(node):
    handle = node.region()

    def refuse(registration):
        raise RuntimeError("no metadata")

    node.transport.partial_metadata = refuse
    with pytest.raises(RuntimeError, match="no metadata"):
        node.service.map(1, handle)
    # A key was handed out for these addresses: they go into quarantine.
    assert node.mapper.unmapped == [handle["offset"]] and node.mapper.discarded == []
    assert node.agent.registered == [] and node.service.holders() == {}


def test_hello_names_the_process_and_the_boot(node):
    assert node.service.hello() == {"node": "mem1", "epoch": "e1", "boot": "b1"}


# --------------------------------------------------------------- front end


def test_requests_get_one_reply_each_with_a_code_on_failure(node):
    handle = node.region()
    ok = handle_request(node.service, 1, {"op": "map", "handle": handle})
    assert ok["ok"] and ok["region_id"] == handle["region_id"]
    assert handle_request(node.service, 1, {"op": "nope"})["code"] == "invalid"
    bad = handle_request(node.service, 1, {"op": "unmap", "region_id": "1"})
    assert bad["code"] == "invalid"
    assert handle_request(node.service, 1, {"op": "map", "handle": {}})["code"] == (
        "invalid"
    )
    wrong = {**handle, "auth_token": handle["auth_token"] + 1}
    assert handle_request(node.service, 2, {"op": "map", "handle": wrong})["code"] == (
        "denied"
    )
    gone = {**node.region(), "auth_token": 1}
    assert handle_request(node.service, 2, {"op": "map", "handle": gone})["code"] == (
        "unavailable"
    )
    released = handle_request(node.service, 1, {"op": "release", "handle": handle})
    assert released == {"ok": True, "released": True}


def test_a_handler_connection_that_closes_drops_its_holds():
    cxl = FakeCxl()
    mem = FakeMemoryNode(cxl)
    try:
        client = RegionMapClient(mem.address, timeout_s=5, key=shared_key())
        handle = cxl.reserve("owner", 4096).to_dict()
        reply = client.map(handle)
        assert reply.epoch == "mem1-e0" and reply.boot == "mem1-boot0"
        assert mem.service.holders() == {handle["region_id"]: {1}}
        client.close()
        wait_for(lambda: mem.service.holders() == {})
    finally:
        mem.stop()


def test_the_client_reconnects_after_a_restart_and_counts_it():
    cxl = FakeCxl()
    mem = FakeMemoryNode(cxl)
    try:
        client = RegionMapClient(mem.address, timeout_s=5, key=shared_key())
        handle = cxl.reserve("owner", 4096).to_dict()
        client.map(handle)
        assert client.generation == 1
        mem.restart(reboot=True)
        reply = client.map(handle)  # the broken connection is opened again
        assert client.generation == 2
        assert reply.epoch == "mem1-e1" and reply.boot == "mem1-boot1"
        client.release(handle)
        assert mem.service.holders() == {}
        with pytest.raises(RegionMapError) as info:
            client.map(handle)
        assert info.value.code == "denied"
        client.close()
    finally:
        mem.stop()


def test_a_release_to_a_restarted_agent_succeeds():
    cxl = FakeCxl()
    mem = FakeMemoryNode(cxl)
    try:
        client = RegionMapClient(mem.address, timeout_s=5, key=shared_key())
        handle = cxl.reserve("owner", 4096).to_dict()
        client.map(handle)
        mem.restart()
        client.release(handle)  # no registration in the new process
        client.close()
    finally:
        mem.stop()


# --------------------------------------------------------- read handles


def test_a_read_handle_shares_a_mapped_region(node):
    handle = node.region()
    owner = node.service.map(1, handle)
    reader = node.service.map(2, read_handle(handle))
    assert reader == owner
    assert node.service.holders() == {handle["region_id"]: {1, 2}}


def test_a_read_handle_cannot_map_a_region_the_first_time(node):
    # Only the region's handle checks it with the resource manager; the
    # owner (or MaruServer) maps it again.
    handle = node.region()
    with pytest.raises(NotMapped):
        node.service.map(2, read_handle(handle))
    assert node.cxl.accesses == []
    reply = handle_request(
        node.service, 2, {"op": "map", "handle": read_handle(handle)}
    )
    assert reply["code"] == "unavailable"


def test_a_read_handle_cannot_release_a_region(node):
    # A reader cannot take a region away from its owner.
    handle = node.region()
    node.service.map(1, handle)
    node.service.map(2, read_handle(handle))
    with pytest.raises(MapDenied, match="read handle cannot release"):
        node.service.release(read_handle(handle))
    reply = handle_request(
        node.service, 2, {"op": "release", "handle": read_handle(handle)}
    )
    assert reply["code"] == "denied"
    assert node.service.holders() == {handle["region_id"]: {1, 2}}
    assert node.agent.registered  # still registered for the owner


def test_a_wrong_read_token_is_denied(node):
    handle = node.region()
    node.service.map(1, handle)
    forged = {**read_handle(handle), "read_token": "0" * 32}
    with pytest.raises(MapDenied, match="read token"):
        node.service.map(2, forged)
    other = node.region()
    with pytest.raises(MapDenied):
        node.service.map(2, {**read_handle(other), "region_id": handle["region_id"]})


def test_a_read_handle_of_a_released_region_is_denied(node):
    handle = node.region()
    node.service.map(1, handle)
    node.service.release(handle)
    with pytest.raises(MapDenied, match="released"):
        node.service.map(2, read_handle(handle))


def test_a_read_handle_does_not_reveal_the_auth_token():
    handle = MaruHandle(region_id=7, offset=0, length=4096, auth_token=123).to_dict()
    reader = read_handle(handle)
    assert set(reader) == {"region_id", "offset", "length", "read_token"}
    assert (
        reader["read_token"] != read_handle({**handle, "auth_token": 124})["read_token"]
    )


# ------------------------------------------------------------------- key


def _raw(mem):
    """A connection that speaks the protocol by hand."""
    import socket

    from maru_remote.region_map import recv_frame, send_frame

    sock = socket.create_connection(("127.0.0.1", int(mem.address.split(":")[1])))

    def ask(message):
        send_frame(sock, message)
        return recv_frame(sock)

    return sock, ask


def test_a_connection_does_nothing_before_it_proves_the_key():
    cxl = FakeCxl()
    mem = FakeMemoryNode(cxl)
    sock, ask = _raw(mem)
    try:
        handle = cxl.reserve("owner", 4096).to_dict()
        hello = ask({"op": "hello"})
        assert hello["ok"] and len(hello["nonce"]) == 16
        for message in (
            {"op": "map", "handle": handle},
            {"op": "release", "handle": handle},
            {"op": "unmap", "region_id": handle["region_id"]},
        ):
            assert ask(message)["code"] == "denied"
        wrong = ask(
            {
                "op": "auth",
                "proof": prove(b"x" * 32, "handler", hello["nonce"]),
                "nonce": b"n" * 16,
            }
        )
        assert wrong["code"] == "denied"
        assert ask({"op": "map", "handle": handle})["code"] == "denied"
        assert cxl.accesses == []
        right = ask(
            {
                "op": "auth",
                "proof": prove(shared_key(), "handler", hello["nonce"]),
                "nonce": b"n" * 16,
            }
        )
        assert right["proof"] == prove(shared_key(), "agent", b"n" * 16)
        assert ask({"op": "map", "handle": handle})["ok"]
    finally:
        sock.close()
        mem.stop()


def test_a_client_with_another_key_cannot_map():
    cxl = FakeCxl()
    mem = FakeMemoryNode(cxl)
    try:
        client = RegionMapClient(mem.address, timeout_s=5, key=b"y" * 32)
        with pytest.raises(RegionMapUnavailable, match="key check failed"):
            client.map(cxl.reserve("owner", 4096).to_dict())
        no_key = RegionMapClient(mem.address, timeout_s=5)
        with pytest.raises(
            RegionMapUnavailable, match="requires the remote CXL agent key"
        ):
            no_key.hello()
        assert cxl.accesses == []
    finally:
        mem.stop()


def test_a_connection_that_never_proves_the_key_is_closed(monkeypatch):
    import maru_remote_cxl_agent.server as server

    monkeypatch.setattr(server, "AUTH_TIMEOUT_S", 0.3)
    mem = FakeMemoryNode(FakeCxl())
    sock, ask = _raw(mem)
    try:
        assert ask({"op": "hello"})["ok"]
        sock.settimeout(5)
        assert sock.recv(1) == b""  # the agent closed it
    finally:
        sock.close()


def test_a_proven_connection_may_stay_idle(monkeypatch):
    import maru_remote_cxl_agent.server as server

    monkeypatch.setattr(server, "AUTH_TIMEOUT_S", 0.3)
    mem = FakeMemoryNode(FakeCxl())
    sock, ask = _raw(mem)
    try:
        hello = ask({"op": "hello"})
        proof = prove(shared_key(), "handler", hello["nonce"])
        assert ask({"op": "auth", "proof": proof, "nonce": b"n" * 16})["ok"]
        time.sleep(0.6)
        assert ask({"op": "hello"})["ok"]
    finally:
        sock.close()


def test_a_connection_that_only_says_hello_is_closed_at_its_deadline(monkeypatch):
    # The deadline counts from the connection, not from its last request.
    import maru_remote_cxl_agent.server as server

    monkeypatch.setattr(server, "AUTH_TIMEOUT_S", 0.3)
    mem = FakeMemoryNode(FakeCxl())
    sock, ask = _raw(mem)
    try:
        deadline = time.monotonic() + 5
        while True:
            assert time.monotonic() < deadline, "still served"
            try:
                if ask({"op": "hello"}) is None:
                    break
            except OSError:
                break
            time.sleep(0.1)
    finally:
        sock.close()


def test_connections_beyond_the_limit_from_one_source_are_closed(monkeypatch):
    import maru_remote_cxl_agent.server as server

    monkeypatch.setattr(server, "MAX_PER_SOURCE", 2)
    mem = FakeMemoryNode(FakeCxl())
    socks = [_raw(mem) for _ in range(3)]
    try:
        assert socks[0][1]({"op": "hello"})["ok"]
        assert socks[1][1]({"op": "hello"})["ok"]
        socks[2][0].settimeout(5)
        assert socks[2][0].recv(1) == b""
    finally:
        for sock, _ in socks:
            sock.close()


def test_running_out_of_file_descriptors_does_not_stop_the_agent():
    import errno

    mem = FakeMemoryNode(FakeCxl())
    real = mem.server._sock

    class Full:
        failed = False

        def accept(self):
            if not self.failed:
                self.failed = True
                raise OSError(errno.EMFILE, "Too many open files")
            return real.accept()

        def __getattr__(self, name):
            return getattr(real, name)

    mem.server._sock = Full()
    sock, ask = _raw(mem)
    try:
        sock.settimeout(5)
        assert ask({"op": "hello"})["ok"]  # accepted after the failed accept
    finally:
        sock.close()


def test_connections_beyond_the_limit_are_closed(monkeypatch):
    import maru_remote_cxl_agent.server as server

    monkeypatch.setattr(server, "MAX_CONNECTIONS", 2)
    mem = FakeMemoryNode(FakeCxl())
    socks = [_raw(mem) for _ in range(3)]
    try:
        assert socks[0][1]({"op": "hello"})["ok"]
        assert socks[1][1]({"op": "hello"})["ok"]
        extra = socks[2][0]
        extra.settimeout(5)
        assert extra.recv(1) == b""
    finally:
        for sock, _ in socks:
            sock.close()


def test_the_client_checks_that_the_remote_cxl_agent_holds_the_key():
    # A process that poses as the remote CXL agent cannot hand out addresses.
    cxl = FakeCxl()
    impostor = FakeMemoryNode(cxl, key=b"z" * 32)
    keyless = FakeMemoryNode(cxl, name="mem2", key=None)
    try:
        for mem in (impostor, keyless):
            client = RegionMapClient(mem.address, timeout_s=5, key=shared_key())
            with pytest.raises(RegionMapUnavailable, match="key check failed"):
                client.hello()
    finally:
        impostor.stop()
        keyless.stop()


def test_an_auth_reply_with_another_proof_is_refused():
    auth = ConnectionAuth(b"k" * 32)
    reply = auth.check(
        {
            "op": "auth",
            "proof": prove(b"k" * 32, "handler", auth.nonce),
            "nonce": b"c" * 16,
        }
    )
    assert reply["ok"] and auth.proved
    assert reply["proof"] != prove(b"other" * 7, "agent", b"c" * 16)


def test_the_key_file_must_hold_a_key_and_stay_private(tmp_path):
    path = tmp_path / "key"
    write_key(str(path))
    assert path.stat().st_mode & 0o777 == 0o600
    assert len(load_key(str(path))) == 32
    with pytest.raises(FileExistsError):
        write_key(str(path))  # an existing key is never replaced
    path.chmod(0o644)
    with pytest.raises(KeyFileError, match="readable by every user"):
        load_key(str(path))
    path.chmod(0o640)
    assert len(load_key(str(path))) == 32
    bad = tmp_path / "bad"
    bad.write_text("not hex\n")
    bad.chmod(0o600)
    with pytest.raises(KeyFileError, match="does not hold"):
        load_key(str(bad))
    with pytest.raises(KeyFileError, match="cannot read"):
        load_key(str(tmp_path / "missing"))


def test_writing_a_key_leaves_no_temporary_file(tmp_path):
    write_key(str(tmp_path / "key"))
    assert [p.name for p in tmp_path.iterdir()] == ["key"]


def test_a_proof_that_is_not_bytes_is_refused():
    import socket

    from maru_remote.region_map import recv_frame, send_frame

    ours, theirs = socket.socketpair()

    def serve():
        recv_frame(theirs)
        send_frame(
            theirs,
            {"ok": True, "node": "n", "epoch": "e", "boot": "", "nonce": b"n" * 16},
        )
        recv_frame(theirs)
        send_frame(theirs, {"ok": True, "proof": 12345})

    threading.Thread(target=serve, daemon=True).start()
    client = RegionMapClient(
        "mem1:9860", timeout_s=5, connect=lambda addr, timeout: ours, key=b"k" * 32
    )
    with pytest.raises(RegionMapUnavailable, match="another key"):
        client.hello()
    theirs.close()


def test_main_refuses_to_start_without_the_key(monkeypatch, tmp_path):
    monkeypatch.setenv("UCX_TLS", "rc")
    argv = ["--rm-address", "mem1:9850", "--lock-file", str(tmp_path / "lock")]
    assert cli.main([*argv, "--key-file", str(tmp_path / "missing")]) == 1
    assert not (tmp_path / "lock").exists()


# ------------------------------------------------------------ bind address


def _sysfs(tmp_path, device, interfaces):
    for name in interfaces:
        (tmp_path / device / "device" / "net" / name).mkdir(parents=True)
    return str(tmp_path)


def test_the_bind_address_is_the_rdma_nics_network_address(tmp_path):
    sysfs = _sysfs(tmp_path, "mlx5_1", ["ens1f1np1"])
    addresses = {"ens1f1np1": "10.0.0.7"}
    assert cli.rdma_address("mlx5_1:1", sysfs, addresses.get) == "10.0.0.7"


def test_no_bind_address_without_a_device_or_an_address(tmp_path):
    with pytest.raises(ValueError, match="MARU_REMOTE_CXL_AGENT_HOST"):
        cli.rdma_address("", str(tmp_path), lambda name: None)
    sysfs = _sysfs(tmp_path, "mlx5_0", ["ens1f0np0"])
    with pytest.raises(ValueError, match="no network interface with an IPv4"):
        cli.rdma_address("mlx5_0:1", sysfs, lambda name: None)
    with pytest.raises(ValueError, match="no network interface"):
        cli.rdma_address("mlx5_9:1", sysfs, lambda name: "10.0.0.1")


def test_main_refuses_to_start_without_a_bind_address(monkeypatch, tmp_path):
    monkeypatch.setenv("UCX_TLS", "rc")
    argv = ["--rm-address", "mem1:9850", "--lock-file", str(tmp_path / "lock")]
    argv += ["--key-file", key_file(), "--host", "", "--ucx-device", ""]
    assert cli.main(argv) == 1
    assert not (tmp_path / "lock").exists()


# ------------------------------------------------------------- command line


@pytest.mark.parametrize(
    "value",
    [
        "rc",
        "rc,dc",
        "rc_x,ud_x:aux",
        "rc_verbs,ud_verbs:aux",
        "dc_mlx5,self",
        "rc,dc,cuda_copy",
        "rc,cuda_copy",
        "dc_mlx5,cuda_copy",
        "rc_x,ud_x:aux,cuda_copy",
    ],
)
def test_transports_that_check_remote_keys_are_accepted(value):
    env = {"UCX_TLS": value}
    assert cli.require_checked_transport(env) == value


# UD has no one-sided RDMA; UCX would carry RDMA over it in software, so it
# passes only as rc's connection-setup helper (":aux"). "ib" enables UD fully.
@pytest.mark.parametrize(
    "value",
    [
        "tcp",
        "rc,tcp",
        "sm",
        "all",
        "^tcp",
        "cma",
        "ib",
        "ud",
        "rc,ud_x",
        "ud_mlx5",
        "cuda",
        "cuda_ipc",
        "rc,cuda",
        "rc,cuda_ipc",
        "rc,cuda_copy,tcp",
        "rc,cuda_copy,sm",
        "rc,cuda_copy,shm",
        "rc,cuda_copy,cuda_ipc",
    ],
)
def test_transports_that_do_not_check_remote_keys_are_refused(value):
    with pytest.raises(cli.UncheckedTransportError):
        cli.require_checked_transport({"UCX_TLS": value})
    assert cli.require_checked_transport({"UCX_TLS": value}, allow_unchecked=True)


def test_an_unset_transport_list_becomes_the_rdma_transports():
    env = {}
    assert cli.require_checked_transport(env) == "rc,dc,cuda_copy"
    assert env["UCX_TLS"] == "rc,dc,cuda_copy"
    assert env["UCX_RCACHE_ENABLE"] == "n"


@pytest.mark.parametrize(
    "value", ["cuda_copy", "self,cuda_copy", "ud_x:aux,cuda_copy", "rc:aux,cuda_copy"]
)
def test_cuda_copy_requires_a_primary_key_checking_remote_transport(value):
    with pytest.raises(cli.UncheckedTransportError, match="without an RC/DC"):
        cli.require_checked_transport({"UCX_TLS": value})
    assert (
        cli.require_checked_transport({"UCX_TLS": value}, allow_unchecked=True) == value
    )


@pytest.mark.parametrize("value", ["y", "try", "yes", "off", "false"])
def test_the_ucx_registration_cache_must_stay_off(value):
    # With it, a NIXL deregistration leaves the NIC registration in place.
    # Only n, no and 0 pass: spellings known to turn the cache off.
    with pytest.raises(cli.UncheckedTransportError, match="RCACHE"):
        cli.require_checked_transport({"UCX_TLS": "rc", "UCX_RCACHE_ENABLE": value})
    env = {"UCX_TLS": "rc", "UCX_RCACHE_ENABLE": "n"}
    assert cli.require_checked_transport(env) == "rc"


def test_main_refuses_an_unchecked_transport_before_starting(monkeypatch, tmp_path):
    monkeypatch.setenv("UCX_TLS", "tcp")
    argv = ["--rm-address", "mem1:9850", "--lock-file", str(tmp_path / "lock")]
    assert cli.main(argv) == 1
    assert not (tmp_path / "lock").exists()  # stopped before the node lock


def test_main_refuses_an_empty_resource_manager_address(monkeypatch, tmp_path):
    # A standalone service's settings leave the address for the operator.
    monkeypatch.setenv("UCX_TLS", "rc")
    monkeypatch.setenv("MARU_RM_ADDRESS", "")
    assert cli.main(["--lock-file", str(tmp_path / "lock")]) == 1
    assert not (tmp_path / "lock").exists()


def test_one_remote_cxl_agent_per_node(monkeypatch, tmp_path):
    path = str(tmp_path / "run" / "lock")
    fd = cli.hold_node_lock(path)
    try:
        with pytest.raises(cli.NodeLockedError):
            cli.hold_node_lock(path)
        monkeypatch.setenv("UCX_TLS", "rc")
        argv = ["--rm-address", "mem1:9850", "--lock-file", path]
        assert cli.main([*argv, "--key-file", key_file()]) == 1
    finally:
        os.close(fd)


def test_boot_id_is_read_from_the_kernel(tmp_path):
    path = tmp_path / "boot_id"
    path.write_text("abc\n")
    assert cli.read_boot_id(str(path)) == "abc"
    assert cli.read_boot_id(str(tmp_path / "missing")) == ""


_ENV_SETTINGS = (
    "MARU_RM_ADDRESS",
    "MARU_REMOTE_CXL_AGENT_HOST",
    "MARU_REMOTE_CXL_AGENT_PORT",
    "MARU_REMOTE_CXL_AGENT_UCX_DEVICE",
    "MARU_REMOTE_CXL_AGENT_LOCK_FILE",
    "MARU_REMOTE_CXL_AGENT_KEY_FILE",
)


def test_the_command_line_defaults(monkeypatch):
    for name in _ENV_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    args = cli.build_parser().parse_args([])
    assert (
        args.rm_address,
        args.host,
        args.port,
        args.ucx_device,
        args.lock_file,
        args.key_file,
        args.allow_unchecked_transport,
    ) == (
        "127.0.0.1:9850",
        "127.0.0.1",
        9860,
        "",
        None,
        "/etc/maru/remote-cxl-agent.key",
        False,
    )


def test_the_service_settings_come_from_the_environment(monkeypatch):
    # The system service passes its settings file as the environment.
    for name, value in zip(
        _ENV_SETTINGS,
        ["mem1:9850", "", "9870", "mlx5_1:1", "/run/maru/x.lock", "/etc/k"],
        strict=True,
    ):
        monkeypatch.setenv(name, value)
    args = cli.build_parser().parse_args([])
    assert (
        args.rm_address,
        args.host,
        args.port,
        args.ucx_device,
        args.lock_file,
        args.key_file,
    ) == ("mem1:9850", "", 9870, "mlx5_1:1", "/run/maru/x.lock", "/etc/k")
    # A command-line option still wins.
    assert cli.build_parser().parse_args(["--port", "9861"]).port == 9861


# ------------------------------------------------------------ device mapper


def test_the_device_mapper_maps_the_region_of_the_local_device(tmp_path):
    device = tmp_path / "dax-local"
    device.write_bytes(bytes(8192))
    mapper = DaxRegionMapper({"uuid-1": str(device)})
    assert mapper.resolve("/dev/dax9.9", "uuid-1") == str(device)
    assert mapper.resolve("/fs/file", "") == "/fs/file"
    addr = mapper.map("/dev/dax9.9", "uuid-1", 4096, 4096)
    ctypes.memmove(addr, b"region", 6)
    mapper.unmap(addr, 4096)
    assert device.read_bytes()[4096:4102] == b"region"


def test_the_device_mapper_refuses_a_device_whose_uuid_is_not_here(
    tmp_path, monkeypatch
):
    """The RM's path may name another device on this node: it is opened only
    when its header carries the region's UUID."""
    from maru_remote_cxl_agent import mapper as mapper_module

    other = tmp_path / "dax-other"
    other.write_bytes(bytes(8192))
    monkeypatch.setattr(mapper_module, "scan_dax_devices", lambda: [])
    headers = {str(other): "uuid-other"}
    monkeypatch.setattr(mapper_module, "read_device_uuid", headers.get)
    mapper = DaxRegionMapper({})
    with pytest.raises(RuntimeError, match="uuid-1 .* not found"):
        mapper.resolve(str(other), "uuid-1")
    with pytest.raises(RuntimeError, match="not found"):
        mapper.map(str(other), "uuid-1", 0, 4096)
    headers[str(other)] = "uuid-1"  # sysfs cannot see it, but the header matches
    assert mapper.resolve(str(other), "uuid-1") == str(other)


def test_an_unmapped_range_is_not_given_to_the_next_mapping(tmp_path):
    device = tmp_path / "dax-local"
    device.write_bytes(bytes(1 << 20))
    now = [0.0]
    mapper = DaxRegionMapper({"u": str(device)}, clock=lambda: now[0])
    first = mapper.map("/dev/x", "u", 0, 1 << 20)
    mapper.unmap(first, 1 << 20)
    # The same bytes map again at another address while the old one is held.
    second = mapper.map("/dev/x", "u", 0, 1 << 20)
    assert second != first
    assert mapper.held_addresses() == [(first, 1 << 20)]
    mapper.unmap(second, 1 << 20)
    # The old 600 s timeout cannot fence a worker paused before posting.
    # Passing a full week must not permit either old address to be reused.
    now[0] = 7 * 24 * 60 * 60
    third = mapper.map("/dev/x", "u", 0, 1 << 20)
    assert third not in {first, second}
    assert mapper.held_addresses() == [(first, 1 << 20), (second, 1 << 20)]
    mapper.unmap(third, 1 << 20)


def test_a_failed_new_map_preserves_the_old_address_reservations(tmp_path, monkeypatch):
    from maru_remote_cxl_agent import mapper as mapper_module

    device = tmp_path / "dax-local"
    device.write_bytes(bytes(4096))
    mapper = DaxRegionMapper({"u": str(device)})
    address = mapper.map("/dev/x", "u", 0, 4096)
    mapper.unmap(address, 4096)

    def no_address_space(*args):
        ctypes.set_errno(12)  # ENOMEM, as for address-space/VMA exhaustion
        return mapper_module._MAP_FAILED

    monkeypatch.setattr(mapper_module._libc, "mmap", no_address_space)
    with pytest.raises(OSError) as error:
        mapper.map("/dev/x", "u", 0, 4096)
    assert error.value.errno == 12
    assert mapper.held_addresses() == [(address, 4096)]


def test_mapper_object_cleanup_does_not_release_registered_addresses(tmp_path):
    device = tmp_path / "dax-local"
    device.write_bytes(bytes(4096))
    mapper = DaxRegionMapper({"u": str(device)})
    address = mapper.map("/dev/x", "u", 0, 4096)
    mapper.unmap(address, 4096)
    del mapper
    gc.collect()
    # Service/object cleanup must not create an address-reuse window while
    # this process and its NIC connections may still be alive.
    with open("/proc/self/maps") as maps:
        for line in maps:
            bounds, permissions, *_ = line.split()
            start, end = (int(value, 16) for value in bounds.split("-"))
            if start <= address < end:
                assert permissions[:3] == "---"
                break
        else:
            pytest.fail("released registered address was no longer reserved")


def test_a_discarded_mapping_is_not_held(tmp_path):
    device = tmp_path / "dax-local"
    device.write_bytes(bytes(1 << 20))
    mapper = DaxRegionMapper({"u": str(device)})
    addr = mapper.map("/dev/x", "u", 0, 1 << 20)
    mapper.discard(addr, 1 << 20)
    assert mapper.held_addresses() == []


def test_the_device_mapper_reports_a_failed_map(tmp_path):
    mapper = DaxRegionMapper({})
    with pytest.raises(OSError):
        mapper.map(str(tmp_path / "missing"), "", 0, 4096)


def test_workers_read_a_key_copy_in_their_home_by_default(tmp_path, monkeypatch):
    """A worker installed with the agent gets its own key in /etc/maru, which
    is not the deployment's; workers and MaruServer read their copy."""
    from maru_common.config import MaruConfig
    from maru_remote.region_map import DEFAULT_COPY_FILE, load_key

    monkeypatch.setenv("HOME", str(tmp_path))
    key = tmp_path / ".maru" / "remote-cxl-agent.key"
    key.parent.mkdir()
    key.write_text("ab" * 16 + "\n")
    key.chmod(0o600)
    assert MaruConfig(auto_connect=False).remote_cxl_agent_key_file == DEFAULT_COPY_FILE
    assert load_key(DEFAULT_COPY_FILE) == bytes.fromhex("ab" * 16)


def test_main_refuses_a_node_without_a_boot_id(monkeypatch, tmp_path):
    monkeypatch.setenv("UCX_TLS", "rc")
    monkeypatch.setattr(cli, "read_boot_id", lambda: "")
    argv = ["--rm-address", "mem1:9850", "--host", "127.0.0.1"]
    lock = str(tmp_path / "lock")
    assert cli.main([*argv, "--lock-file", lock, "--key-file", key_file()]) == 1
    assert not (tmp_path / "lock").exists()  # stopped before the node lock


def test_a_worker_refuses_an_agent_without_a_boot_id():
    """Without it a reboot of the memory node, which loses writes its CPU
    caches held, could not be told from a restart of the agent."""
    cxl = FakeCxl()
    mem = FakeMemoryNode(cxl)
    try:
        mem.service.boot_id = ""
        client = RegionMapClient(mem.address, timeout_s=5, key=shared_key())
        with pytest.raises(RegionMapUnavailable, match="no boot id"):
            client.map(cxl.reserve("owner", 4096).to_dict())
    finally:
        mem.stop()
