# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: N802 -- Fakes use the public native binding's method spellings.
"""Public native adapter contracts, using no native NIXL or device calls."""

import ast
import gc
import logging
import os
import subprocess
import sys
from enum import Enum
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from maru_handler.storage.remote_cxl.datapath import _default_transport
from maru_remote import native_agent
from maru_remote.native_agent import NativeNixlAgent
from maru_remote.transport import NixlTransport, TransferTimeout, TransferUncertainError
from tests.unit.remote_fakes import FakeNixlAgent, reset_fake_agents


class NativeError(RuntimeError):
    pass


class RegList:
    def __init__(self, mem_type, descs):
        self.mem_type = mem_type
        self.items = list(descs)
        if any(not isinstance(d, tuple) or len(d) != 4 for d in self.items):
            raise ValueError("native registration requires four-tuples")


class XferList:
    def __init__(self, mem_type, descs):
        self.mem_type = mem_type
        self.items = list(descs)
        if any(not isinstance(d, tuple) or len(d) != 3 for d in self.items):
            raise ValueError("native transfer requires three-tuples")


class NativeConfig:
    pass


class NativeAgent:
    """Translate public native calls to the existing owned-DRAM transport fake."""

    instances: list["NativeAgent"] = []

    def __init__(self, name, config):
        self.name, self.config = name, config
        self.inner = FakeNixlAgent(name)
        self.calls = []
        self.registrations = {}
        self.requests = {}
        self.released = []
        self.query_error = self.release_error = self.post_error = None
        self.post_status = self.query_status = None
        self.next_handle = 4096
        self.backend_handle = 2048
        type(self).instances.append(self)

    def getAvailPlugins(self):
        return ["UCX"]

    def createBackend(self, backend, params):
        self.calls.append(("create", backend, params))
        self.inner.create_backend(backend, params)
        return self.backend_handle

    def getBackendParams(self, handle):
        assert handle == self.backend_handle
        return {}, ["DRAM"]

    def registerMem(self, descs, backends):
        assert backends == []
        self.calls.append(("register", descs.items))
        self.registrations[id(descs)] = self.inner.register_memory(descs.items, "DRAM")

    def deregisterMem(self, descs, backends):
        assert backends == []
        self.inner.deregister_memory(self.registrations.pop(id(descs)))

    def getLocalMD(self):
        return self.inner.get_agent_metadata()

    def getLocalPartialMD(self, descs, include, backends):
        assert backends == []
        return self.inner.get_partial_agent_metadata(
            self.registrations[id(descs)], include
        )

    def loadRemoteMD(self, metadata):
        return self.inner.add_remote_agent(metadata)

    def invalidateRemoteMD(self, name):
        self.inner.remove_remote_agent(name)

    def genNotif(self, name, message, backends=()):
        assert not backends
        self.inner.send_notif(name, message)

    def getNotifs(self, existing, backends):
        assert existing == {} and backends == []
        return self.inner.get_new_notifs()

    def createXferReq(self, operation, local, remote, peer, notification, backends):
        assert notification == b"" and backends == []
        self.calls.append(("prepare", operation, local.items, remote.items, peer))
        self.next_handle += 1
        self.requests[self.next_handle] = self.inner.initialize_xfer(
            "READ" if operation == 1 else "WRITE", local.items, remote.items, peer
        )
        return self.next_handle

    @staticmethod
    def state(value):
        if value == "ERR":
            # The real binding throws for negative native status.
            raise NativeError("NIXL_ERR_BACKEND")
        return 0 if value == "DONE" else 1

    def postXferReq(self, handle, message):
        assert message == b""
        status = self.state(self.inner.transfer(self.requests[handle]))
        if self.post_error is not None:
            raise self.post_error
        return status if self.post_status is None else self.post_status

    def getXferStatus(self, handle):
        if self.query_error is not None:
            raise self.query_error
        status = self.state(self.inner.check_xfer_state(self.requests[handle]))
        return status if self.query_status is None else self.query_status

    def releaseXferReq(self, handle):
        if self.release_error is not None:
            raise self.release_error
        self.inner.release_xfer_handle(self.requests.pop(handle))
        self.released.append(handle)


# Simulate the selected wheel's public type origins for the loader guard.
for native_type in [NativeAgent, RegList, XferList]:
    native_type.__module__ = "nixl_cu13._bindings"


@pytest.fixture
def bindings(monkeypatch):
    reset_fake_agents()
    NativeAgent.instances = []
    api = ModuleType("nixl")
    for name, value in {
        "nixlAgent": NativeAgent,
        "nixlAgentConfig": NativeConfig,
        "nixlRegDList": RegList,
        "nixlXferDList": XferList,
        "NIXL_READ": 1,
        "NIXL_WRITE": 2,
        "NIXL_SUCCESS": 0,
        "NIXL_IN_PROG": 1,
        "NIXL_THREAD_SYNC_NONE": 0,
        "NIXL_THREAD_SYNC_STRICT": 1,
        "NIXL_THREAD_SYNC_RW": 2,
        "NIXL_THREAD_SYNC_DEFAULT": 1,
        "DRAM_SEG": 0,
        "VRAM_SEG": 1,
        "FILE_SEG": 2,
        "BLK_SEG": 3,
        "OBJ_SEG": 4,
    }.items():
        setattr(api, name, value)
    monkeypatch.setitem(sys.modules, "nixl", api)
    monkeypatch.setattr(native_agent, "version", lambda name: "1.2.0")
    yield api
    reset_fake_agents()


def _make(name):
    agent = NativeNixlAgent(name)
    agent.create_backend("UCX", {})
    return NixlTransport(name, agent=agent), NativeAgent.instances[-1]


def _pair():
    a, native_a = _make("a")
    b, native_b = _make("b")
    local, remote = bytearray(64), bytearray(range(64))
    from maru_remote.transport import buffer_address

    la, ra = buffer_address(local), buffer_address(remote)
    a.register(la, len(local))
    b.register(ra, len(remote))
    peer = a.add_peer(b.metadata())
    return a, b, native_a, native_b, local, remote, la, ra, peer


def test_importing_adapter_does_not_import_nixl_or_torch():
    result = subprocess.run(
        [
            sys.executable,
            "-S",
            "-c",
            "import sys; import maru_remote.native_agent; "
            "assert 'nixl' not in sys.modules; assert 'torch' not in sys.modules",
        ],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_worker_factory_enables_event_progress_without_replacing_an_agent(bindings):
    transport = _default_transport(SimpleNamespace(rdma_device="nic:1"))("worker")
    native = NativeAgent.instances[-1]
    assert native.config.useProgThread is True
    assert native.config.pthrDelay == 1000
    assert native.config.useListenThread is False
    assert native.config.syncMode == bindings.NIXL_THREAD_SYNC_NONE
    assert native.calls == [("create", "UCX", {"ucx_devices": "nic:1"})]
    assert len(NativeAgent.instances) == 1
    transport.close()


def test_worker_factory_creates_ucx_even_with_default_device(bindings):
    transport = _default_transport(SimpleNamespace(rdma_device=""))("worker")
    assert NativeAgent.instances[-1].calls == [("create", "UCX", {})]
    transport.close()


@pytest.mark.parametrize("found", ["1.1.0", "2.0.0", "1.2.0rc1"])
def test_unsupported_version_is_an_actionable_import_error(
    bindings, monkeypatch, found
):
    monkeypatch.setattr(native_agent, "version", lambda name: found)
    with pytest.raises(ImportError, match="nixl>=1.2.0,<2"):
        NativeNixlAgent("unsupported")
    assert not NativeAgent.instances


@pytest.mark.parametrize("found", ["1.2.0", "1.3.0", "1.5.0", "1.17.3"])
def test_matching_supported_1x_versions_keep_public_capability_checks(
    bindings, monkeypatch, found
):
    monkeypatch.setattr(native_agent, "version", lambda name: found)
    NativeNixlAgent("supported")
    assert len(NativeAgent.instances) == 1


def test_missing_native_method_is_an_actionable_import_error(bindings, monkeypatch):
    monkeypatch.delattr(NativeAgent, "getLocalPartialMD")
    with pytest.raises(
        ImportError, match="native agent methods missing.*getLocalPartialMD"
    ):
        NativeNixlAgent("missing-native-method")
    assert not NativeAgent.instances


def test_mismatched_meta_and_cuda_wheel_fail_before_agent_creation(
    bindings, monkeypatch
):
    monkeypatch.setattr(
        native_agent, "version", lambda n: "1.2.0" if n == "nixl" else "1.2.1"
    )
    with pytest.raises(ImportError, match="wheel versions differ"):
        NativeNixlAgent("mismatch")
    assert not NativeAgent.instances


def test_missing_native_public_export_fails_without_private_import(
    bindings, monkeypatch
):
    monkeypatch.delattr(bindings, "nixlAgentConfig")
    with pytest.raises(ImportError, match="public exports missing: nixlAgentConfig"):
        NativeNixlAgent("missing")


@pytest.mark.parametrize("timeout", [-1, 2147483648, 1.5])
def test_invalid_event_timeout_never_creates_an_agent(bindings, timeout):
    with pytest.raises(ValueError, match="event_timeout_us"):
        NativeNixlAgent("bad-delay", event_timeout_us=timeout)
    assert not NativeAgent.instances


def test_multi_range_order_and_tag_bytes_round_trip(bindings):
    a, b, native, _, local, remote, la, ra, peer = _pair()
    # Each side has a different non-monotonic order; independent sorting corrupts pairing.
    a.read(peer, [(la + 32, ra, 16), (la, ra + 48, 16)])
    assert local[32:48] == remote[:16] and local[:16] == remote[48:]
    prepare = next(call for call in native.calls if call[0] == "prepare")
    assert prepare[2] == [(la + 32, 16, 0), (la, 16, 0)]
    assert prepare[3] == [(ra, 16, 0), (ra + 48, 16, 0)]
    assert native.released == [4097]
    a.close()
    b.close()


def test_wrong_native_descriptor_kinds_are_rejected_before_iteration(bindings):
    adapter = NativeNixlAgent("descriptors")
    native = NativeAgent.instances[-1]
    with pytest.raises(TypeError, match="registration requires RegDList"):
        adapter.register_memory(XferList(0, [(100, 8, 0)]), "DRAM")
    with pytest.raises(TypeError, match="transfer requires XferDList"):
        adapter.get_xfer_descs(RegList(0, [(100, 8, 0, "")]), "DRAM")
    assert not native.calls and not native.requests


def test_partial_metadata_and_notification_match_transport_contract(bindings):
    a, _ = _make("a")
    b, _ = _make("b")
    from maru_remote.transport import buffer_address

    first, second, local = bytearray(range(16)), bytearray(range(16, 32)), bytearray(16)
    r1, r2, la = buffer_address(first), buffer_address(second), buffer_address(local)
    a.register(la, 16)
    registration1, registration2 = b.register(r1, 16, "one"), b.register(r2, 16, "two")
    peer = a.add_peer(b.partial_metadata(registration1))
    a.read(peer, [(la, r1, 16)])
    assert local == first
    a.add_peer(b.partial_metadata(registration2), peer=peer)
    a.read(peer, [(la, r2, 16)])
    assert local == second
    assert a.announce(peer) and b.take_notifs() == {"a": [b""]}
    assert b.take_notifs() == {}
    a.close()
    b.close()


@pytest.mark.parametrize("where", ["query", "post"])
def test_native_exception_keeps_the_posted_handle_and_metadata(bindings, where):
    a, b, native, _, local, remote, la, ra, peer = _pair()
    native.inner.stall = True
    problem = NativeError("injected native failure")
    if where == "query":
        native.query_error = problem
    else:
        native.post_error = problem
    with pytest.raises(TransferUncertainError) as failure:
        a.read(peer, [(la, ra, 64)])
    pending = failure.value.pending
    assert failure.value.__cause__ is problem
    assert not pending.finished and not native.released
    assert not a.disconnect(peer)
    assert peer in native.inner.peers
    assert local != remote
    with pytest.raises(RuntimeError, match="still own registered memory"):
        a.close()
    native.query_error = native.post_error = None
    native.inner.finish_stalled()
    assert pending.poll() and pending.completed and local == remote
    assert native.released == [4097]
    a.close()
    b.close()


def test_release_failure_is_retried_without_marking_handle_released(bindings):
    a, b, native, _, local, remote, la, ra, peer = _pair()
    native.release_error = NativeError("release uncertain")
    with pytest.raises(TransferUncertainError) as failure:
        a.read(peer, [(la, ra, 64)])
    pending = failure.value.pending
    assert pending.completed and not pending.finished
    assert not pending.handle.released and local == remote
    native.release_error = None
    assert pending.poll() and pending.handle.released
    assert pending.poll() and native.released == [4097]
    a.close()
    b.close()


@pytest.mark.parametrize("where", ["post_status", "query_status"])
def test_unknown_native_positive_status_keeps_the_post_and_buffers(bindings, where):
    a, b, native, _, local, remote, la, ra, peer = _pair()
    native.inner.stall = True
    setattr(native, where, 2)  # 1.2 and 1.5 define only SUCCESS=0 and IN_PROG=1.
    with pytest.raises(
        TransferUncertainError, match="without confirmed cancellation"
    ) as failure:
        a.read(peer, [(la, ra, 64)])
    assert "unexpected NIXL native transfer status 2" in str(failure.value.__cause__)
    pending = failure.value.pending
    assert not pending.finished and not pending.completed and not native.released
    assert not a.disconnect(peer)
    assert peer in native.inner.peers and local != remote
    setattr(native, where, None)
    native.inner.finish_stalled()
    assert pending.poll() and pending.completed and local == remote
    a.close()
    b.close()


def test_descriptor_generator_failure_cannot_free_a_previous_post(bindings):
    from maru_remote.transport import buffer_address

    adapter = NativeNixlAgent("a")
    adapter.create_backend("UCX", {})
    native = NativeAgent.instances[-1]
    a = NixlTransport("a", agent=adapter)
    b, _ = _make("b")
    local, remote = bytearray(64), bytearray(range(64))
    la, ra = buffer_address(local), buffer_address(remote)
    a.register(la, 64)
    b.register(ra, 64)
    peer = a.add_peer(b.metadata())
    native.inner.stall = True
    with pytest.raises(TransferTimeout) as timeout:
        a.read(peer, [(la, ra, 64)], timeout_s=0.001)
    pending = timeout.value.pending

    def broken():
        yield (la, 16, 0)
        raise ValueError("descriptor source failed")

    # A different descriptor batch fails while the prior RMA remains live.
    with pytest.raises(ValueError, match="descriptor source failed"):
        adapter.get_xfer_descs(broken(), "DRAM")
    gc.collect()
    assert not native.released and not pending.finished and len(native.requests) == 1
    native.inner.finish_stalled()
    assert pending.poll() and local == remote
    a.close()
    b.close()


def test_opaque_handle_retains_native_owner_without_automatic_release(bindings):
    adapter = NativeNixlAgent("owner")
    native = NativeAgent.instances[-1]
    native.inner.peers.add("owner")
    local, remote = XferList(0, [(100, 1, 0)]), XferList(0, [(200, 1, 0)])
    # Prepare-only fake descriptors: no address is read or written in this test.
    native.inner.initialize_xfer = lambda *args: []
    handle = adapter.initialize_xfer("READ", local, remote, "owner")
    del adapter
    gc.collect()
    assert handle.owner is native and not native.released
    del handle
    gc.collect()
    assert not native.released


def _wrapper_sources():
    from importlib.metadata import PackageNotFoundError, distribution

    paths = []
    try:
        paths.append(Path(distribution("nixl-cu13").locate_file("nixl_cu13/_api.py")))
    except PackageNotFoundError:
        pass
    paths.extend(
        Path(p)
        for p in os.environ.get("MARU_TEST_NIXL_API_SOURCES", "").split(os.pathsep)
        if p
    )
    return list(dict.fromkeys(paths)) or [None]


def _installed_wrapper(api, path):
    """Use installed wrapper class bodies against fake native types, without imports."""
    if path is None or not path.is_file():
        pytest.skip("installed NIXL 1.2 Python source unavailable")
    wanted = {
        "nixl_agent_config",
        "nixl_agent",
        "nixl_xfer_handle",
        "nixl_prepped_dlist_handle",
        "nixl_thread_sync_t",
    }
    nodes = [
        n
        for n in ast.parse(path.read_text()).body
        if isinstance(n, ast.ClassDef) and n.name in wanted
    ]
    namespace = {
        "nixlBind": api,
        "Enum": Enum,
        "DEFAULT_COMM_PORT": 5555,
        "logger": logging.getLogger(__name__),
    }
    code = compile(
        ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
        str(path),
        "exec",
        flags=__import__("__future__").annotations.compiler_flag,
    )
    exec(code, namespace)
    return namespace


@pytest.mark.parametrize("source", _wrapper_sources())
def test_installed_wrapper_and_adapter_agree_on_prepost_wire_arguments(
    bindings, source
):
    ns = _installed_wrapper(bindings, source)
    wrapper = ns["nixl_agent"]("wrapped", ns["nixl_agent_config"](backends=[]))
    adapter = NativeNixlAgent("adapted")
    wrapper_native, adapter_native = NativeAgent.instances[-2:]
    for obj in [wrapper, adapter]:
        obj.create_backend("UCX", {"ucx_devices": "nic:1"})
        # Native preparation is intercepted so these addresses are never accessed.
        native = wrapper_native if obj is wrapper else adapter_native
        native.inner.initialize_xfer = lambda *args: []
        registration = obj.register_memory([(900, 32, 0, "tag")], "DRAM")
        desc = obj.get_xfer_descs([(912, 4, 0), (900, 8, 0)], "DRAM")
        handle = obj.initialize_xfer("WRITE", desc, desc, "peer")
        obj.release_xfer_handle(handle)
        obj.release_xfer_handle(handle)  # explicit release is idempotent
        obj.deregister_memory(registration)
    assert wrapper_native.calls == adapter_native.calls
    assert wrapper_native.released == adapter_native.released == [4097]
