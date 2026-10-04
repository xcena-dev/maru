# SPDX-License-Identifier: Apache-2.0
"""Gates G0/G1 for the remote storage backend, through the MaruHandler API.

Opens ``MaruHandler(storage_backend="remote")`` against a pool node's
maru-remote-server and, per round, stores N objects of random bytes, checks
that they exist, stores the same keys again with different bytes (the first
value must win), retrieves them, compares SHA-256 digests and releases the
leases. It reports RDMA bandwidth (bytes over the transfer time the backend
measured) and end-to-end call time, plus the server's reservation, quarantine
and ticket counts after the run (all must be 0).

Usage (on a worker node):
  PYTHONPATH=. python tools/remote_storage_g0.py --remote-url tcp://pool-node:6600 \
      --ucx-device mlx5_0:1 --objects 64 --object-bytes 4M --rounds 3 --out g0.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import uuid

from maru_common.config import MaruConfig
from maru_handler import MaruHandler
from maru_remote.__main__ import parse_size


def _payload(n: int, seed: int) -> bytes:
    return hashlib.shake_256(seed.to_bytes(8, "little")).digest(n)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--remote-url", required=True)
    ap.add_argument("--ucx-device", default="")
    ap.add_argument("--objects", type=int, default=64)
    ap.add_argument("--object-bytes", type=parse_size, default=parse_size("4M"))
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--namespace", default=f"g0-{uuid.uuid4().hex[:8]}")
    ap.add_argument("--out", default="g0.json")
    args = ap.parse_args()

    staging = args.objects * args.object_bytes
    h = MaruHandler(
        MaruConfig(
            storage_backend="remote",
            remote_url=args.remote_url,
            remote_ucx_device=args.ucx_device,
            cache_namespace=args.namespace,
            pool_size=staging,
            chunk_size_bytes=args.object_bytes,
            remote_load_reserve=0.0,  # every object is stored before any is read
            auto_connect=False,
            timeout_ms=5000,
        )
    )
    h.connect()
    backend = h._storage  # the tool reads the backend's transfer counters
    rounds = []
    ok_all = True
    try:
        for r in range(args.rounds):
            keys = [f"r{r}-o{i}" for i in range(args.objects)]
            data = [
                _payload(args.object_bytes, r * 1_000_003 + i)
                for i in range(args.objects)
            ]
            handles = []
            for blob in data:
                a = h.alloc(len(blob))
                a.buf[:] = blob
                handles.append(a)
            w0 = backend.counters["write_seconds"]
            t0 = time.perf_counter()
            stored = h.batch_store(keys, handles)
            t1 = time.perf_counter()
            write_s = backend.counters["write_seconds"] - w0
            exists = h.batch_exists(keys)
            dup = []
            for key in keys[:4]:  # a second writer of the same keys
                a = h.alloc(16)
                a.buf[:] = b"\xff" * 16
                dup.append(h.batch_store([key], [a])[0])
            r0 = backend.counters["read_seconds"]
            t2 = time.perf_counter()
            leases = h.batch_retrieve(keys)
            t3 = time.perf_counter()
            read_s = backend.counters["read_seconds"] - r0
            match = [
                lease is not None
                and hashlib.sha256(lease.view).digest() == hashlib.sha256(blob).digest()
                for lease, blob in zip(leases, data, strict=True)
            ]
            h.release_retrieved(leases)
            nbytes = args.objects * args.object_bytes
            row = {
                "round": r,
                "bytes": nbytes,
                "stored": sum(stored),
                "exists": sum(exists),
                "duplicate_reported_present": sum(dup),
                "sha256_match": sum(match),
                "write_s": write_s,
                "read_s": read_s,
                "store_call_s": t1 - t0,
                "retrieve_call_s": t3 - t2,
                "write_gbps": nbytes / write_s / 1e9 if write_s else None,
                "read_gbps": nbytes / read_s / 1e9 if read_s else None,
            }
            ok = row["stored"] == row["exists"] == row[
                "sha256_match"
            ] == args.objects and row["duplicate_reported_present"] == len(dup)
            ok_all &= ok
            rounds.append(row)
            print(
                f"round {r}: {nbytes / 1e6:.1f} MB  write {row['write_gbps']:.2f} GB/s"
                f"  read {row['read_gbps']:.2f} GB/s  store call {row['store_call_s'] * 1e3:.1f} ms"
                f"  retrieve call {row['retrieve_call_s'] * 1e3:.1f} ms  sha256 {row['sha256_match']}/{args.objects}"
                f"  {'OK' if ok else 'FAIL'}"
            )
        server = h.get_stats()["remote_storage"].get("server", {})
        leftovers = {
            k: server.get(k) for k in ("reservations", "quarantined", "tickets")
        }
        ok_all &= all(v == 0 for v in leftovers.values())
        total_bytes = sum(x["bytes"] for x in rounds)
        summary = {
            "remote_url": args.remote_url,
            "ucx_device": args.ucx_device,
            "objects": args.objects,
            "object_bytes": args.object_bytes,
            "rounds": rounds,
            "write_gbps_total": total_bytes / sum(x["write_s"] for x in rounds) / 1e9,
            "read_gbps_total": total_bytes / sum(x["read_s"] for x in rounds) / 1e9,
            "server_after": leftovers,
            "regions": server.get("regions"),
            "pid": os.getpid(),
            "ok": bool(ok_all),
        }
        print(
            f"total: write {summary['write_gbps_total']:.2f} GB/s, read "
            f"{summary['read_gbps_total']:.2f} GB/s, server after {leftovers}, "
            f"regions {summary['regions']} -> {'PASS' if ok_all else 'FAIL'}"
        )
        with open(args.out, "w") as f:
            json.dump(summary, f, indent=2)
    finally:
        h.close()
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
