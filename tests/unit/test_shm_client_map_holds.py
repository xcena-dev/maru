# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient holds every region it maps (MAP_HOLD) until it unmaps it.

The resource manager keeps a freed or reclaimed region's range out of use
while it is held, so the hold must be there before the mapping and must
outlive it. A mock resource manager records what the client sends.
"""

import errno
import logging
import os
import struct
import threading

import pytest

from maru_shm.client import MaruShmClient
from maru_shm.constants import PROT_READ, PROT_WRITE
from maru_shm.ipc import (
    HEADER_SIZE,
    ErrorResp,
    FreeResp,
    GetAccessResp,
    MsgHeader,
    MsgType,
)
from maru_shm.types import MaruHandle
from maru_shm.uds_helpers import read_full, write_full

from .test_shm_client import MockResourceManagerServer

_RW = PROT_READ | PROT_WRITE


class HoldingRm:
    """A resource manager that answers MAP_HOLD, UNHOLD, GET_ACCESS and FREE.

    ``requests`` lists what arrived, in order: ("map_hold", region, holder,
    boot_id, pid, pid_start, pid_ns, client_id), ("unhold", region, holder,
    instance), ("get_access", region), ("free", region), ("node_free", region).
    """

    def __init__(self, tmp_path, map_hold_status=0, supports_holds=True):
        self.path = str(tmp_path / "device")
        with open(self.path, "wb") as f:
            f.write(b"\xa5" * 8192)
        self.map_hold_status = map_hold_status
        self.supports_holds = supports_holds
        self.unhold_statuses = []  # Answered first, one per UNHOLD; then 0.
        self.requests = []
        self._lock = threading.Lock()
        self._server = MockResourceManagerServer(self._serve)
        self.address = self._server.start()

    def stop(self):
        self._server.stop()

    def kinds(self):
        with self._lock:
            return [r[0] for r in self.requests]

    def _record(self, *request):
        with self._lock:
            self.requests.append(request)

    def _reply(self, sock, msg_type, payload):
        hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
        write_full(sock, hdr.pack() + payload)

    def _access(self, status=0):
        if status != 0:
            return GetAccessResp(status=status).pack()
        return GetAccessResp(status=0, dax_path=self.path, offset=0, length=4096).pack()

    def _serve(self, sock):
        while True:
            try:
                hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
                payload = read_full(sock, hdr.payload_len)
            except (ConnectionError, OSError):
                return
            region = struct.unpack_from("<Q", payload)[0] if payload else 0
            if hdr.msg_type == MsgType.MAP_HOLD_REQ:
                if not self.supports_holds:
                    self._record("map_hold", region)
                    err = ErrorResp(status=-errno.ENOSYS, message="unknown request")
                    self._reply(sock, MsgType.ERROR_RESP, err.pack())
                    return  # As the RM does: it closes the connection.
                holder_len, boot_len, pid, _, pid_start, pid_ns = struct.unpack_from(
                    "<IIIIQQ", payload, 32
                )
                off = 64
                holder = payload[off : off + holder_len].decode()
                off += holder_len
                boot_id = payload[off : off + boot_len].decode()
                off += boot_len
                (cid_len,) = struct.unpack_from("<H", payload, off)
                client_id = payload[off + 2 : off + 2 + cid_len].decode()
                assert len(payload) == off + 2 + cid_len + 8  # Then the request ID.
                self._record(
                    "map_hold",
                    region,
                    holder,
                    boot_id,
                    pid,
                    pid_start,
                    pid_ns,
                    client_id,
                )
                self._reply(
                    sock, MsgType.MAP_HOLD_RESP, self._access(self.map_hold_status)
                )
            elif hdr.msg_type == MsgType.UNHOLD_REQ:
                holder_len, instance_len = struct.unpack_from("<II", payload, 32)
                holder = payload[40 : 40 + holder_len].decode()
                instance = payload[
                    40 + holder_len : 40 + holder_len + instance_len
                ].decode()
                self._record("unhold", region, holder, instance)
                status = self.unhold_statuses.pop(0) if self.unhold_statuses else 0
                self._reply(sock, MsgType.UNHOLD_RESP, FreeResp(status=status).pack())
            elif hdr.msg_type == MsgType.GET_ACCESS_REQ:
                self._record("get_access", region)
                self._reply(sock, MsgType.GET_ACCESS_RESP, self._access())
            elif hdr.msg_type == MsgType.FREE_REQ:
                self._record("free", region)
                self._reply(sock, MsgType.FREE_RESP, FreeResp(status=0).pack())
            elif hdr.msg_type == MsgType.NODE_FREE_REQ:
                self._record("node_free", region)
                self._reply(sock, MsgType.NODE_FREE_RESP, FreeResp(status=0).pack())


@pytest.fixture
def rm(tmp_path):
    server = HoldingRm(tmp_path)
    yield server
    server.stop()


@pytest.fixture
def shm_log(caplog, monkeypatch):
    """Warnings of maru_shm, also after setup_logging() stopped propagation."""
    monkeypatch.setattr(logging.getLogger("maru_shm"), "propagate", True)
    with caplog.at_level(logging.WARNING, logger="maru_shm.client"):
        yield caplog


def _handle(region_id=7):
    return MaruHandle(region_id=region_id, offset=0, length=4096, auth_token=42)


def _pid_start():
    with open("/proc/self/stat") as f:
        return int(f.read().rsplit(")", 1)[1].split()[19])


def test_mmap_holds_first_and_munmap_unholds(rm):
    client = MaruShmClient(address=rm.address)
    try:
        mm = client.mmap(_handle(), _RW)
        assert mm[0:1] == b"\xa5"
        # One request: the hold answers what GET_ACCESS would.
        assert rm.kinds() == ["map_hold"]
        _, region, holder, boot_id, pid, pid_start, pid_ns, client_id = rm.requests[0]
        assert region == 7
        assert client_id == client._client_id
        assert pid == os.getpid()
        assert pid_start == _pid_start()
        assert pid_ns == os.stat("/proc/self/ns/pid").st_ino
        with open("/proc/sys/kernel/random/boot_id") as f:
            assert boot_id == f.read().strip()
        # A name the resource manager accepts: printable ASCII, no spaces.
        assert holder.startswith("mmap:") and len(holder) <= 128
        assert all(0x21 <= ord(c) <= 0x7E for c in holder)

        assert client.mmap(_handle(), _RW) is mm  # Cached: no second hold.
        assert rm.kinds() == ["map_hold"]

        client.munmap(_handle())
        assert mm.closed
        assert rm.requests[1:] == [("unhold", 7, holder, holder)]
        client.munmap(_handle())  # Not mapped: nothing to drop.
        assert len(rm.requests) == 2
    finally:
        client.close()


def test_close_and_free_drop_the_holds(rm):
    client = MaruShmClient(address=rm.address)
    client.mmap(_handle(1), _RW)
    client.mmap(_handle(2), _RW)
    client.mmap(_handle(3), _RW)
    # The owner frees first; the resource manager drains until the unhold.
    client.free(_handle(2))
    assert rm.kinds()[3:] == ["free", "unhold"]
    assert rm.requests[4][1] == 2
    client.close()
    assert sorted(r[1] for r in rm.requests[5:]) == [1, 3]
    assert rm.kinds()[5:] == ["unhold", "unhold"]
    client.close()
    assert len(rm.requests) == 7


def test_node_free_drops_the_hold(rm):
    client = MaruShmClient(address=rm.address)
    try:
        client.mmap(_handle(), _RW)
        client.node_free("gpu-node", "epoch-1", _handle())
        assert rm.kinds() == ["map_hold", "node_free", "unhold"]
        client.close()
        assert len(rm.requests) == 3
    finally:
        client.close()


def test_two_clients_of_one_process_hold_separately(rm):
    a = MaruShmClient(address=rm.address)
    b = MaruShmClient(address=rm.address)
    try:
        a.mmap(_handle(), _RW)
        b.mmap(_handle(), _RW)
        holders = [r[2] for r in rm.requests]
        assert holders[0] != holders[1]
        a.munmap(_handle())
        # b still maps the region under its own hold.
        assert rm.requests[2] == ("unhold", 7, holders[0], holders[0])
    finally:
        a.close()
        b.close()


def test_a_mapping_that_cannot_be_closed_keeps_its_hold(rm):
    client = MaruShmClient(address=rm.address)
    mm = client.mmap(_handle(), _RW)
    exported = memoryview(mm)
    try:
        with pytest.raises(BufferError):
            client.munmap(_handle())
        client.close()
        # The process can still write through `exported`: no unhold, also
        # not when the region is mapped and unmapped once more.
        assert rm.kinds() == ["map_hold"]
        client.mmap(_handle(), _RW)
        client.munmap(_handle())
        client.close()
        assert rm.kinds() == ["map_hold", "map_hold"]
    finally:
        exported.release()
        mm.close()


@pytest.mark.parametrize("status", [-errno.ENOENT, -errno.EIO])
def test_a_refused_hold_maps_nothing(tmp_path, status):
    rm = HoldingRm(tmp_path, map_hold_status=status)
    client = MaruShmClient(address=rm.address)
    try:
        with pytest.raises(RuntimeError, match=f"Map hold failed with status {status}"):
            client.mmap(_handle(), _RW)
        assert client._mmap_cache == {}
        # A hold that may be recorded but is not durable is dropped again.
        assert rm.kinds() == (
            ["map_hold"] if status == -errno.ENOENT else ["map_hold", "unhold"]
        )
    finally:
        client.close()
        rm.stop()


def test_a_failed_mapping_drops_its_hold(rm):
    os.unlink(rm.path)
    client = MaruShmClient(address=rm.address)
    try:
        with pytest.raises(FileNotFoundError):
            client.mmap(_handle(), _RW)
        assert rm.kinds() == ["map_hold", "unhold"]
    finally:
        client.close()


def test_a_resource_manager_without_map_holds_is_asked_once(tmp_path, shm_log):
    rm = HoldingRm(tmp_path, supports_holds=False)
    client = MaruShmClient(address=rm.address)
    try:
        client.mmap(_handle(1), _RW)
        client.mmap(_handle(2), _RW)
        assert rm.kinds() == ["map_hold", "get_access", "get_access"]
        client._map_holds_retry_at = 0.0  # Later: it is asked once more.
        client.mmap(_handle(3), _RW)
        assert rm.kinds()[3:] == ["map_hold", "get_access"]
        warnings = [r for r in shm_log.records if "does not hold mapped" in r.message]
        assert len(warnings) == 1
        client.munmap(_handle(1))
        client.close()
        assert "unhold" not in rm.kinds()
    finally:
        rm.stop()


def test_an_upgraded_resource_manager_holds_what_the_client_maps(tmp_path):
    rm = HoldingRm(tmp_path, supports_holds=False)
    client = MaruShmClient(address=rm.address)
    try:
        client.mmap(_handle(1), _RW)
        client.mmap(_handle(2), _RW)
        client.munmap(_handle(2))
        assert rm.kinds() == ["map_hold", "get_access", "get_access"]
        # The resource manager is restarted with a version that has MAP_HOLD.
        rm.supports_holds = True
        client._map_holds_retry_at = 0.0
        client.mmap(_handle(3), _RW)
        # Region 3 is held before it is mapped; region 1, still mapped
        # without a hold, is held now; region 2 is no longer mapped.
        assert [r[:2] for r in rm.requests[3:]] == [("map_hold", 3), ("map_hold", 1)]
        client.close()
        assert rm.kinds()[5:] == ["unhold", "unhold"]
        assert sorted(r[1] for r in rm.requests[5:]) == [1, 3]
    finally:
        rm.stop()


def test_unhold_failure_does_not_fail_munmap(rm, shm_log):
    client = MaruShmClient(address=rm.address)
    mm = client.mmap(_handle(), _RW)
    _lose_rm(client)
    client.munmap(_handle())
    client.close()
    assert mm.closed
    assert any("could not drop the hold" in r.message for r in shm_log.records)


def _lose_rm(client):
    """The resource manager goes away: nothing listens at its address."""
    address = client._address
    client._address = "127.0.0.1:1"
    with client._conn_lock:
        client._close_conn()
    return address


def test_unholds_the_rm_missed_follow_its_next_answer(rm):
    client = MaruShmClient(address=rm.address)
    try:
        for region in (1, 2, 3):
            client.mmap(_handle(region), _RW)
        address = _lose_rm(client)
        client.munmap(_handle(1))  # Fails, and starts the back-off.
        client.munmap(_handle(2))  # Within the back-off: not even tried.
        assert rm.kinds() == ["map_hold"] * 3
        client._address = address
        # Still within the back-off, but the resource manager answers again.
        client.get_access(_handle(3))
        assert rm.kinds()[3:] == ["get_access", "unhold", "unhold"]
        assert sorted(r[1] for r in rm.requests[4:]) == [1, 2]
        client.get_access(_handle(3))  # Nothing pending any more.
        assert len(rm.requests) == 7
    finally:
        client.close()


def test_close_drops_the_holds_the_rm_missed(rm):
    client = MaruShmClient(address=rm.address)
    client.mmap(_handle(1), _RW)
    client.mmap(_handle(2), _RW)
    address = _lose_rm(client)
    client.munmap(_handle(1))
    client._address = address
    client.close()
    assert rm.kinds()[2:] == ["unhold", "unhold"]
    assert sorted(r[1] for r in rm.requests[2:]) == [1, 2]


def test_a_missed_unhold_is_dropped_when_the_region_is_mapped_again(rm):
    client = MaruShmClient(address=rm.address)
    try:
        client.mmap(_handle(), _RW)
        address = _lose_rm(client)
        client.munmap(_handle())
        client._address = address
        client.mmap(_handle(), _RW)
        client.get_access(_handle())
        # The region is mapped: its hold was asked for again, never dropped.
        assert rm.kinds() == ["map_hold", "map_hold", "get_access"]
    finally:
        client.close()
    assert rm.kinds()[3:] == ["unhold"]


def test_an_unhold_the_rm_could_not_save_is_sent_again(rm):
    client = MaruShmClient(address=rm.address)
    client.mmap(_handle(1), _RW)
    client.mmap(_handle(2), _RW)
    rm.unhold_statuses = [-errno.EIO]  # The RM keeps the hold.
    client.munmap(_handle(1))
    client.munmap(_handle(2))  # Waits behind the first: not sent yet.
    client.get_access(_handle(2))  # An answer does not end this wait.
    assert rm.kinds()[2:] == ["unhold", "get_access"]
    client.close()
    assert rm.kinds()[4:] == ["unhold", "unhold"]
    assert sorted(r[1] for r in rm.requests[4:]) == [1, 2]


def test_an_unhold_for_a_region_that_is_gone_is_not_sent_again(rm):
    client = MaruShmClient(address=rm.address)
    client.mmap(_handle(), _RW)
    rm.unhold_statuses = [-errno.ENOENT]
    client.munmap(_handle())
    client.close()
    assert rm.kinds() == ["map_hold", "unhold"]


def test_close_closes_the_other_regions_when_one_cannot_be_closed(rm):
    client = MaruShmClient(address=rm.address)
    first = client.mmap(_handle(1), _RW)
    second = client.mmap(_handle(2), _RW)
    exported = memoryview(first)
    try:
        with pytest.raises(BufferError):
            client.close()
        assert second.closed and not first.closed
        # Region 1 can still be written through `exported`: it stays held.
        assert rm.requests[2:] == [("unhold", 2, rm.requests[1][2], rm.requests[1][2])]
        client.close()  # Nothing left to close or to report.
        assert len(rm.requests) == 3
    finally:
        exported.release()
        first.close()


def test_a_forked_child_cannot_map_with_the_parents_client(rm):
    client = MaruShmClient(address=rm.address)
    try:
        mm = client.mmap(_handle(1), _RW)
        pid = client._pid
        client._pid += 1  # The same client as a forked child sees it.
        assert client.mmap(_handle(1), _RW) is mm  # Inherited: still readable.
        with pytest.raises(RuntimeError, match="created before fork"):
            client.mmap(_handle(2), _RW)
        assert rm.kinds() == ["map_hold"]
        client._pid = pid
    finally:
        client.close()


def test_a_forked_child_does_not_drop_the_parents_hold(rm):
    parent = MaruShmClient(address=rm.address)
    parent.mmap(_handle(), _RW)
    # The same client as a forked child sees it: another pid.
    parent._pid += 1
    parent.munmap(_handle())
    parent.close()
    assert rm.kinds() == ["map_hold"]


def test_munmap_cannot_unhold_a_region_mapped_again(rm):
    """An unhold never lands after another thread mapped the region again."""
    client = MaruShmClient(address=rm.address)
    try:
        for _ in range(50):
            client.mmap(_handle(), _RW)
            other = threading.Thread(target=client.mmap, args=(_handle(), _RW))
            other.start()
            client.munmap(_handle())
            other.join()
            # Mapped at the end means held at the end.
            if 7 in client._mmap_cache:
                last = [k for k in rm.kinds() if k != "get_access"][-1]
                assert last == "map_hold"
                client.munmap(_handle())
    finally:
        client.close()
