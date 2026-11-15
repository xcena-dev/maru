# SPDX-License-Identifier: Apache-2.0
"""A GPU node [dram, remote, ssd] on a real MaruServer, with a memory node and
the fake NIC: DRAM demotes to the memory node first and to its SSD what the
memory node cannot take (none ready, the quarantine cap, no room, every WRITE
slot taken, part of the credit, a WRITE not posted); write-through makes one
copy the same way; a source with a copy in either tier is finalized onto it;
a source whose write there may still commit never goes to the SSD; the two
tiers never move pages between them."""

import shutil
import tempfile
import time

import pytest

from maru_common.remote_protocol import TransferEnd
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    REMOTE_CAPABILITY,
    STAGE_CAPABILITY,
    Key,
)
from maru_server.catalog import PoolState
from maru_storage_service.config import parse_node_config
from maru_storage_service.drivers import peer as peer_module
from maru_storage_service.page_table import PageRef
from tests.unit.ms_node_helpers import (
    PAGE,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_demotions_applied,
    wait_until,
)
from tests.unit.remote_fakes import key_file, reset_fake_agents
from tests.unit.test_ms_demotion import SMALL_CREDIT, put, value_of
from tests.unit.test_ms_remote_demotion import memory_config
from tests.unit.test_ms_remote_demotion import ms as ms
from tests.unit.test_ms_remote_demotion import workers as workers
from tests.unit.test_ms_remote_tier import fake_transport, free_port
from tests.unit.test_ms_ssd_tier import ssd_bytes

NS = "ns"
TO_REMOTE, TO_SSD = "move_dram_to_remote", "move_dram_to_ssd"
# A memory node that credits a peer up to 12 pages at once
WIDE_CREDIT = {
    "credit_max_bytes": 12 * PAGE,
    "credit_total_max_bytes": 12 * PAGE,
    "credit_refill_low": PAGE,
    "credit_refill_high": 2 * PAGE,
    "commit_pending_max_bytes": 4 * PAGE,
    "transfer_reserve_bytes": PAGE,
}


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


def gpu_config(url, sockdir, port, ssd_dir, *, demotion="back", **remote):
    return parse_node_config(
        {
            "node_id": "gpu-test",
            "socket": f"{sockdir}/node.sock",
            "server": url,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "dram",
                    "size": 16 * PAGE,
                    "hugepage": False,
                    "high_watermark": 0.5,
                    "low_watermark": 0.25,
                    "demotion": demotion,
                },
                {
                    "store": "remote",
                    "url": [f"127.0.0.1:{port}"],
                    "key_file": key_file(),
                }
                | remote,
                {"store": "ssd", "size": 64 * PAGE, "path": ssd_dir},
            ],
            "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE},
        }
    )


class Trio:
    """A memory node (64 pages) and a GPU node [dram, remote, ssd] on one
    MaruServer."""

    def __init__(self, server, url, *, memory=True, wide=True, **gpu):
        self.server, self.url, self.wide = server, url, wide
        self.sockdir = short_tmpdir()
        self.ssd_dir = tempfile.mkdtemp(prefix="maru-l2-", dir="/var/tmp")
        self.port = free_port()
        self.memory = None
        if memory:
            self.start_memory()
        self.gpu = start_node(
            gpu_config(url, self.sockdir, self.port, self.ssd_dir, **gpu),
            make_transport=fake_transport,
        )
        self.remote = self.gpu.tier("remote")
        self.manager = self.gpu.manager
        if memory:
            wait_until(lambda: self.remote.ready("mem1"), what="remote ready")

    def start_memory(self):
        config = memory_config(self.url, self.port, pages=64)
        if self.wide:
            config = parse_node_config(
                {
                    "node_id": "mem1",
                    "role": "memory",
                    "server": self.url,
                    "page_size": PAGE,
                    "remote_server": {
                        "listen": f"127.0.0.1:{self.port}",
                        "key_file": key_file(),
                    },
                    "tiers": [{"store": "dram", "size": 64 * PAGE, "hugepage": False}],
                    "budgets": WIDE_CREDIT,
                }
            )
        self.memory = start_node(config, make_transport=fake_transport)

    def close(self):
        self.remote.transport.stall(False)
        self.remote.transport.finish_stalled()
        self.gpu.stop()
        if self.memory is not None:
            self.memory.stop()
        remove_tree(self.sockdir)
        shutil.rmtree(self.ssd_dir, ignore_errors=True)

    def where(self, key: str) -> set[str]:
        """Where the key is: "dram" and "ssd" of the GPU node, "memory"."""
        service = self.server.ms_service
        with service.lock:
            locations = [
                e.location
                for e in service.catalog.locations(Key(NS, key))
                if e.pool.state == PoolState.ACTIVE
            ]
        return {
            "memory" if loc.pool_id.split(":")[1] == "mem1" else str(loc.kind)
            for loc in locations
        }

    def written(self, monkeypatch) -> list[tuple[set[str], set[str]]]:
        """Per demotion pass, the keys of the sources it wrote to the memory
        node and to the SSD (write-back only)."""
        m = self.manager
        passes: list[tuple[set[str], set[str]]] = []
        demote, write, run = m._demote, m._write_remote, m._run_moves

        def demote_spy(*args):
            passes.append((set(), set()))
            demote(*args)

        def write_spy(tier, move):
            posted = write(tier, move)
            if posted and passes and move.through is None:
                passes[-1][0].add(move.key.key)
            return posted

        def run_spy(moves, io=False):
            if passes:
                passes[-1][1].update(
                    mv.key.key for mv in moves if mv.through is None and not mv.existing
                )
            run(moves, io=io)

        monkeypatch.setattr(m, "_demote", demote_spy)
        monkeypatch.setattr(m, "_write_remote", write_spy)
        monkeypatch.setattr(m, "_run_moves", run_spy)
        return passes


@pytest.fixture
def trio(ms):
    server, url = ms
    made: list[Trio] = []

    def make(**kwargs) -> Trio:
        made.append(Trio(server, url, **kwargs))
        return made[-1]

    yield make
    for t in made:
        t.close()


def put_all(handler, start: int, count: int) -> list[str]:
    keys = [f"k{i}" for i in range(start, start + count)]
    for key in keys:
        assert put(handler, key)
    return keys


def settled(t: Trio) -> None:
    wait_demotions_applied(t.manager)


def unposted(m, passes: list) -> list[tuple[int, str]]:
    """Records (pass, key) of each write-back WRITE that was not posted."""
    out: list[tuple[int, str]] = []
    write = m._write_remote

    def spy(tier, move):
        posted = write(tier, move)
        if not posted:
            out.append((len(passes) - 1, move.key.key))
        return posted

    m._write_remote = spy
    return out


def offers(m, monkeypatch) -> set[str]:
    """Records the keys each write-back pass offers the remote tier."""
    out: set[str] = set()
    demote = m._demote_remote

    def spy(name, pools, tier, refs, items, spill=False):
        out.update(item.key.key for item in items)
        return demote(name, pools, tier, refs, items, spill)

    monkeypatch.setattr(m, "_demote_remote", spy)
    return out


def through_batches(m, size: int, monkeypatch) -> None:
    """Write-through copies wait until ``size`` pages are READY: one batch."""
    copy = m._copy_through

    def batched():
        with m._lock:
            ready = len(set(m._through))
        if ready >= size:
            copy()

    monkeypatch.setattr(m, "_copy_through", batched)


def room_at_reserve(remote, monkeypatch) -> None:
    """RESERVE credits as if the quarantine had room: by the WRITE another
    transfer took it."""
    monkeypatch.setattr(remote, "_usable", lambda: True)
    monkeypatch.setattr(remote, "_fits", lambda: 1 << 20)


# =============================================================================
# Write-back: remote first, the SSD for what it cannot take
# =============================================================================


def test_a_ready_memory_node_takes_the_demotions(trio, workers):
    t = trio()
    # The node asks for and offers what it does with either L2 tier alone
    assert t.gpu.capabilities() == [REMOTE_CAPABILITY, STAGE_CAPABILITY]
    assert FETCH_CAPABILITY in t.gpu.local.capabilities
    assert t.manager.traits["dram"].demotes_to == ("remote", "ssd")
    keys = put_all(workers(t.gpu), 0, 12)
    m = t.manager
    wait_until(lambda: m.move_results["RETIRED"] >= 3, what="demoted")
    settled(t)
    moved = [k for k in keys if t.where(k) == {"memory"}]
    assert len(moved) >= 3
    assert m.moved_bytes[TO_REMOTE] == m.move_results["COMMIT:ADDED"] * 3000
    assert TO_SSD not in m.moved_bytes
    assert not any("ssd" in t.where(k) for k in keys)
    t.gpu.pools.check()


def test_with_no_memory_node_ready_the_ssd_takes_them_until_it_is_back(
    trio, workers, monkeypatch
):
    monkeypatch.setattr(peer_module, "RECONNECT_S", 0.1)
    t = trio(memory=False)
    m = t.manager
    h = workers(t.gpu)
    first = put_all(h, 0, 12)
    wait_until(lambda: m.move_results["RETIRED"] >= 3, what="demoted")
    settled(t)
    on_ssd = [k for k in first if t.where(k) == {"ssd"}]
    assert len(on_ssd) >= 3 and m.moved_bytes[TO_SSD] >= 3 * 3000
    assert TO_REMOTE not in m.moved_bytes
    # The memory node comes: new demotions go there again
    t.start_memory()
    wait_until(lambda: t.remote.ready("mem1"), what="remote ready")
    to_ssd = m.moved_bytes[TO_SSD]
    second = put_all(h, 12, 12)
    wait_until(lambda: m.moved_bytes[TO_REMOTE] >= 3 * 3000, what="to remote")
    settled(t)
    assert any(t.where(k) == {"memory"} for k in second)
    assert m.moved_bytes[TO_SSD] == to_ssd
    # What went to the SSD stays there: no move between the L2 tiers
    assert all(t.where(k) == {"ssd"} for k in on_ssd)
    t.gpu.pools.check()


def test_while_the_memory_node_is_gone_the_ssd_takes_them_then_remote_again(
    trio, workers, monkeypatch
):
    monkeypatch.setattr(peer_module, "RECONNECT_S", 0.1)
    t = trio()
    m = t.manager
    h = workers(t.gpu)
    put_all(h, 0, 12)
    wait_until(lambda: m.moved_bytes[TO_REMOTE] >= 3 * 3000, what="to remote")
    settled(t)
    assert TO_SSD not in m.moved_bytes
    t.memory.stop()
    wait_until(lambda: not t.remote.ready("mem1"), what="remote lost")
    to_remote = m.moved_bytes[TO_REMOTE]
    gone = put_all(h, 12, 12)
    wait_until(lambda: m.moved_bytes[TO_SSD] >= 3 * 3000, what="to the SSD")
    settled(t)
    on_ssd = [k for k in gone if t.where(k) == {"ssd"}]
    assert len(on_ssd) >= 3 and m.moved_bytes[TO_REMOTE] == to_remote
    # A new run of the memory node: demotions go there again
    t.start_memory()
    wait_until(lambda: t.remote.ready("mem1"), what="remote ready again")
    to_ssd = m.moved_bytes[TO_SSD]
    back = put_all(h, 24, 12)
    wait_until(lambda: m.moved_bytes[TO_REMOTE] > to_remote, what="to remote again")
    settled(t)
    assert any(t.where(k) == {"memory"} for k in back)
    assert m.moved_bytes[TO_SSD] == to_ssd
    assert all(t.where(k) == {"ssd"} for k in on_ssd)
    t.gpu.pools.check()


def test_at_the_quarantine_cap_the_ssd_takes_them(trio, workers):
    t = trio()
    t.remote.quarantine_max = 0  # this node's cap: no transfer fits
    keys = put_all(workers(t.gpu), 0, 12)
    m = t.manager
    wait_until(lambda: m.move_results["RETIRED"] >= 3, what="demoted")
    settled(t)
    assert m.moved_bytes[TO_SSD] >= 3 * 3000 and TO_REMOTE not in m.moved_bytes
    assert not any("memory" in t.where(k) for k in keys)
    assert not m.move_results["DROPPED"]


def test_with_no_room_on_the_memory_node_the_ssd_takes_them(trio, workers):
    t = trio()
    t.memory.peer_ops.quarantine_max = 0  # it gives this peer no room
    keys = put_all(workers(t.gpu), 0, 12)
    m = t.manager
    wait_until(lambda: m.move_results["RETIRED"] >= 3, what="demoted")
    settled(t)
    assert m.moved_bytes[TO_SSD] >= 3 * 3000 and TO_REMOTE not in m.moved_bytes
    assert not any("memory" in t.where(k) for k in keys)
    assert not m.move_results["DROPPED"]


def test_with_every_write_slot_taken_the_ssd_takes_them(trio, workers):
    t = trio()
    t.remote.transport.stall()  # writes stay in flight
    keys = put_all(workers(t.gpu), 0, 16)
    m = t.manager
    slots = t.remote.io_depth
    wait_until(
        lambda: m._remote_writing == slots and m.moved_bytes[TO_SSD] > 0,
        what="slots taken, the rest to the SSD",
    )
    assert TO_REMOTE not in m.moved_bytes
    t.remote.transport.stall(False)
    assert t.remote.transport.finish_stalled() == slots
    wait_until(lambda: m.moved_bytes[TO_REMOTE] >= slots * 3000, what="committed")
    settled(t)
    assert not any(t.where(k) >= {"memory", "ssd"} for k in keys)
    t.gpu.pools.check()


def test_a_pass_with_part_of_the_credit_splits_between_the_tiers(
    trio, workers, monkeypatch
):
    # The memory node credits a peer 2 pages at a time
    t = trio(wide=False)
    passes = t.written(monkeypatch)
    keys = put_all(workers(t.gpu), 0, 12)
    m = t.manager
    wait_until(
        lambda: any(remote and ssd for remote, ssd in passes), what="a split pass"
    )
    # A page goes to one of them
    assert all(not remote & ssd for remote, ssd in passes)
    settled(t)
    assert m.moved_bytes[TO_REMOTE] > 0 and m.moved_bytes[TO_SSD] > 0
    assert not any(t.where(k) >= {"memory", "ssd"} for k in keys)
    t.gpu.pools.check()


def test_a_kept_write_keeps_its_source_from_the_ssd_until_it_ends(
    trio, workers, monkeypatch
):
    # Two writes fill this node's quarantine cap; kept, they keep it full
    t = trio(timeout_s=0.3, quarantine_max=2 * PAGE)
    m = t.manager
    kept: dict[str, PageRef] = {}
    ended = m._remote_write_ended
    offered = offers(m, monkeypatch)

    def spy(move, result):
        if result.kept:
            kept[move.key.key] = move.src
        ended(move, result)

    monkeypatch.setattr(m, "_remote_write_ended", spy)
    t.remote.transport.stall()
    h = workers(t.gpu)
    put_all(h, 0, 12)
    wait_until(lambda: len(kept) == 2, what="two writes kept")
    backoff = m.placement._backoff
    wait_until(lambda: all(r in backoff for r in kept.values()), what="released")
    assert m.move_results["COPY_FAILED"] >= 2
    assert all(t.where(k) == {"dram"} for k in kept)  # the sources stay
    # Past their backoff, under pressure again, passes take them once more:
    # the remote tier cannot, and the SSD must not (a late DONE would commit
    # them there too)
    time.sleep(t.gpu.kept_backoff_s)
    offered.clear()
    put_all(h, 12, 8)
    wait_until(lambda: set(kept) <= offered, what="kept sources offered again")
    wait_until(lambda: m.moved_bytes[TO_SSD] > 0, what="others on the SSD")
    settled(t)
    assert not any("ssd" in t.where(k) for k in kept)
    # The late DONE commits them on the memory node, counted as moved
    t.remote.transport.stall(False)
    assert t.remote.transport.finish_stalled() == 2
    wait_until(lambda: all("memory" in t.where(k) for k in kept), what="late commits")
    wait_until(lambda: m.moved_bytes[TO_REMOTE] >= 2 * 3000, what="counted")
    settled(t)
    assert not any("ssd" in t.where(k) for k in kept)
    assert t.remote.kept_bytes == 0
    t.gpu.pools.check()


def test_an_unanswered_write_done_keeps_its_sources_from_the_ssd(
    trio, workers, monkeypatch
):
    t = trio()
    m = t.manager
    offered = offers(m, monkeypatch)
    lost: set[str] = set()
    write_done = t.remote.write_done

    def lose_answer(items):
        outcomes = write_done(items)  # the memory node commits them
        if not lost and items[0].end == TransferEnd.DONE:
            lost.update(item.key.key for item in items)
            t.remote.quarantine_max = 0  # and this node posts nothing more
            raise TimeoutError("the answer is lost")
        return outcomes

    monkeypatch.setattr(t.remote, "write_done", lose_answer)
    h = workers(t.gpu)
    put_all(h, 0, 12)
    wait_until(lambda: lost, what="an answer lost")
    assert all("memory" in t.where(k) for k in lost)
    # Pressure takes the sources again: the remote tier cannot, and the SSD
    # must not (they may be on the memory node already)
    time.sleep(t.gpu.kept_backoff_s)
    offered.clear()
    put_all(h, 12, 8)
    wait_until(lambda: m.moved_bytes[TO_SSD] > 0, what="others on the SSD")

    def offered_again() -> bool:
        m.want_room("dram", 16 * PAGE)
        return lost <= offered

    wait_until(offered_again, what="those sources offered again")
    settled(t)
    assert not any("ssd" in t.where(k) for k in lost)
    # The remote tier takes pages again: their demotion lands on that copy
    t.remote.quarantine_max = 1 << 30

    def onto_the_copy() -> bool:
        m.want_room("dram", 16 * PAGE)  # demote every page
        return all(t.where(k) == {"memory"} for k in lost)

    wait_until(onto_the_copy, what="onto the copy")
    settled(t)
    assert not any("ssd" in t.where(k) for k in lost)
    t.gpu.pools.check()


def test_a_write_not_posted_sends_its_page_to_the_ssd(trio, workers, monkeypatch):
    # Credit past the quarantine room: the WRITEs past it are not posted
    t = trio(quarantine_max=2 * PAGE)
    room_at_reserve(t.remote, monkeypatch)
    m = t.manager
    passes = t.written(monkeypatch)
    missed = unposted(m, passes)
    t.remote.transport.stall()
    keys = put_all(workers(t.gpu), 0, 12)
    wait_until(lambda: missed, what="a WRITE not posted")
    # The SSD takes those pages (in the same pass, as far as its I/O slots go)
    wait_until(
        lambda: all(t.where(key) == {"ssd"} for _, key in missed),
        what="those pages on the SSD",
    )
    assert any(key in passes[i][1] for i, key in missed)
    assert not m.move_results["COPY_FAILED"]
    t.remote.transport.stall(False)
    assert t.remote.transport.finish_stalled() == 2
    settled(t)
    assert m.moved_bytes[TO_REMOTE] >= 2 * 3000
    assert not any(t.where(k) >= {"memory", "ssd"} for k in keys)
    t.gpu.pools.check()


# =============================================================================
# Write-through: one copy, remote first
# =============================================================================


def test_write_through_copies_to_the_memory_node_first(trio, workers):
    t = trio(demotion="through")
    put_all(workers(t.gpu), 0, 3)
    m = t.manager
    wait_until(
        lambda: all(t.where(f"k{i}") == {"dram", "memory"} for i in range(3)),
        what="copies on the memory node",
    )
    settled(t)
    assert m.through_results["COMMIT:ADDED"] >= 3 and not m._copies
    assert m.moved_bytes[TO_REMOTE] == 3 * 3000 and TO_SSD not in m.moved_bytes


def test_write_through_falls_back_to_the_ssd_and_pressure_finalizes_onto_either(
    trio, workers, monkeypatch
):
    t = trio(demotion="through")
    m = t.manager
    t.remote.quarantine_max = 0
    h = workers(t.gpu)
    first = put_all(h, 0, 3)
    wait_until(
        lambda: all(t.where(k) == {"dram", "ssd"} for k in first),
        what="copies on the SSD",
    )
    assert len(m._copies) == 3 and not m._remote_copies
    assert m.through_results["COMMIT:ADDED"] == 3
    # Remote takes copies again; the sources with an SSD copy are only
    # finalized onto it, those with a copy there onto that
    t.remote.quarantine_max = 1 << 30
    passes = t.written(monkeypatch)
    second = put_all(h, 3, 9)
    wait_until(
        lambda: all(t.where(k) == {"ssd"} for k in first), what="finalized onto SSD"
    )
    wait_until(
        lambda: any(t.where(k) == {"memory"} for k in second),
        what="finalized onto the memory node",
    )
    written = set().union(*(remote | ssd for remote, ssd in passes))
    assert not written & set(first)
    settled(t)
    t.gpu.pools.check()


@pytest.mark.parametrize("credit", ["fitting", "past the room"])
def test_write_through_past_the_quarantine_room_copies_the_rest_to_the_ssd(
    trio, workers, monkeypatch, credit
):
    # Room for two transfers and a batch of three READY pages: RESERVE gives
    # two credits, or three of which the last WRITE is not posted; the third
    # copy goes to the SSD either way
    t = trio(demotion="through", quarantine_max=2 * PAGE)
    m = t.manager
    if credit == "past the room":
        room_at_reserve(t.remote, monkeypatch)
    missed = unposted(m, [])
    through_batches(m, 3, monkeypatch)
    t.remote.transport.stall()
    keys = put_all(workers(t.gpu), 0, 3)
    wait_until(
        lambda: m._remote_writing == 2 and len(m._copies) == 1,
        what="two copies in flight, one on the SSD",
    )
    t.remote.transport.stall(False)
    assert t.remote.transport.finish_stalled() == 2
    wait_until(lambda: m.through_results["COMMIT:ADDED"] == 3, what="three copies")
    settled(t)
    assert all(len(t.where(k) - {"dram"}) == 1 for k in keys)
    assert sum("memory" in t.where(k) for k in keys) == 2
    assert not m.through_results["COPY_FAILED"]
    assert len(missed) == (0 if credit == "fitting" else 1)


def test_the_values_on_either_tier_are_the_keys_bytes(trio, workers):
    t = trio(wide=False)
    keys = put_all(workers(t.gpu), 0, 12)
    m = t.manager
    wait_until(
        lambda: m.moved_bytes[TO_REMOTE] > 0 and m.moved_bytes[TO_SSD] > 0,
        what="both tiers took pages",
    )
    settled(t)
    ssd = t.gpu.tier("ssd").pool
    service = t.server.ms_service
    for key in keys:
        with service.lock:
            entries = list(service.catalog.locations(Key(NS, key)))
        for entry in entries:
            loc = entry.location
            if loc.pool_id.split(":")[1] == "mem1":
                data = t.memory.transfers._maps[loc.pool_id]
                got = bytes(data[loc.offset : loc.offset + loc.length])
            elif str(loc.kind) == "ssd":
                got = ssd_bytes(ssd, loc)
            else:
                continue
            assert got == value_of(key), key
