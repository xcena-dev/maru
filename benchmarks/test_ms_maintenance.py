# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Opt-in latency benchmark: run without coverage on an otherwise idle host.

    python -m pytest benchmarks/test_ms_maintenance.py -q

The default suite verifies bounded batches and concurrent lookup progress in
test_ms_registry.py; this benchmark retains the 10 ms p99 regression budget.
"""

import time

from maru_common.storage_types import CloseMode
from maru_server.catalog import Entry
from tests.unit.ms_fakes import PAGE, FakeNode, FakeWorker, Wire, key, make_service


def test_large_forced_sweep_keeps_lookup_p99_low():
    service = make_service(clock=time.monotonic, maintenance_interval_s=0.01)
    wire = Wire(service)
    reader = FakeNode(wire, "reader")
    reader.open()
    keys = [key(f"r{i}") for i in range(32)]
    reader.commit(*[(k, reader.loc(reader.dram(), i)) for i, k in enumerate(keys)])
    worker = FakeWorker(wire, reader)
    worker.open()
    big = FakeNode(wire, "big")
    size = 300_000
    big.open(pools=[big.dram(pages=size)])
    pool = service.catalog.pool(big.dram_id)
    for i in range(size):
        loc = big.loc(big.dram(pages=size), i)
        service.catalog._add(Entry(key(f"b{i}"), loc, PAGE, pool))

    def lookup():
        response = worker.lookup(*keys)
        worker.unpin(*[item.pin_id for item in response.items])

    def p99(samples):
        return sorted(samples)[int(len(samples) * 0.99)]

    def measure(n):
        samples = []
        for _ in range(n):
            t0 = time.perf_counter()
            lookup()
            samples.append(time.perf_counter() - t0)
        return samples

    base = measure(300)
    big.close(CloseMode.LEAVE)
    service.ensure_started()
    try:
        loaded = measure(300)
        assert len(pool.entries) < size
        deadline = time.monotonic() + 30
        while pool.entries and time.monotonic() < deadline:
            loaded += measure(20)
    finally:
        service.stop()
    assert not pool.entries
    assert p99(loaded) < p99(base) + 0.010, (p99(base), p99(loaded))
