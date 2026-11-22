# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""A worker process for tests: one MaruHandler on maru_node, driven by JSON lines
on stdin (bytes as hex).

    python ms_worker_agent.py <socket> <engine_id> <namespace> <chunk_bytes>
"""

import json
import logging
import os
import sys
import traceback


def main() -> int:
    socket_path, engine, namespace, chunk = sys.argv[1:5]
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    import maru_handler
    from maru_common.config import MaruConfig
    from maru_handler import MaruHandler

    if os.environ.get("AGENT_CXL_PATHS"):
        _map_cxl_from(json.loads(os.environ["AGENT_CXL_PATHS"]))

    handler = MaruHandler(
        MaruConfig(
            maru_node=socket_path,
            engine_id=engine,
            cache_namespace=namespace,
            chunk_size_bytes=int(chunk),
            pool_size=int(chunk),
            pin_pools=False,
            timeout_ms=int(os.environ.get("AGENT_TIMEOUT_MS", "500")),
            store_timeout_s=float(os.environ.get("AGENT_STORE_TIMEOUT_S", "10")),
            auto_connect=False,
        )
    )
    client = handler._cpu
    if os.environ.get("AGENT_READ_ONLY") == "1":
        client._refill_once = lambda: False  # a rank that never stores
    reads = {}
    held = {}
    for line in sys.stdin:
        command = json.loads(line)
        op = command.pop("op")
        try:
            if op == "connect":
                handler.connect()
                result = {
                    "incarnation": client.identity.incarnation,
                    "pid": os.getpid(),
                    "node_epoch": client.node_epoch,
                    "module": maru_handler.__file__,
                }
            elif op == "store":
                value = bytes.fromhex(command["value"])
                handle = handler.alloc(len(value))
                handle.buf[:] = value
                result = handler.store(command["key"], handle)
            elif op == "store_many":
                keys, handles = [], []
                for key, value in command["items"]:
                    value = bytes.fromhex(value)
                    try:
                        handle = handler.alloc(len(value))
                    except ValueError:
                        break
                    handle.buf[:] = value
                    keys.append(key)
                    handles.append(handle)
                result = [len(keys), handler.batch_store(keys, handles)]
            elif op == "retrieve":
                read = handler.retrieve(command["key"])
                if read is None:
                    result = None
                else:
                    if command.get("hold", True):
                        reads[command["key"]] = read
                        result = bytes(read.view).hex()
                    else:
                        result = bytes(read.view).hex()
                        handler.release_retrieved([read])
            elif op == "held":
                result = bytes(reads[command["key"]].view).hex()
            elif op == "release":
                handler.release_retrieved([reads.pop(command["key"])])
                result = True
            elif op == "alloc_hold":
                handle = handler.alloc(command["size"])
                handle.buf[:] = b"\xab" * command["size"]
                held[command["name"]] = handle
                result = True
            elif op == "free_held":
                handler.free(held.pop(command["name"]))
                result = True
            elif op == "exists":
                result = handler.batch_exists(command["keys"])
            elif op == "state":
                result = {
                    "node_epoch": client.node_epoch,
                    "storage_connected": client.storage_connected,
                    "unused_credit": client.credits.unused_bytes,
                    "pools": client.pools.pool_ids(),
                    "stats": dict(client.stats),
                    "off": None if client.off is None else str(client.off),
                    "server_epoch": client.server_conn.server_epoch
                    if client.server_conn
                    else None,
                }
            elif op == "wait_credit":
                import time

                deadline = time.monotonic() + command.get("wait_s", 10)
                while client.credits.unused_bytes < command["bytes"]:
                    if time.monotonic() > deadline:
                        raise TimeoutError("no credit")
                    time.sleep(0.01)
                result = client.credits.unused_bytes
            elif op == "close":
                handler.release_retrieved(list(reads.values()))
                reads.clear()
                handler.close()
                result = True
            elif op == "close_holding":
                handler.close()  # reads stay held
                result = True
            elif op == "exit":
                print(json.dumps({"ok": True, "result": True}), flush=True)
                return 0
            else:
                raise ValueError(f"unknown op {op}")
            answer = {"ok": True, "result": result}
        except Exception as e:
            traceback.print_exc()
            answer = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        print(json.dumps(answer), flush=True)
    return 0


def _map_cxl_from(paths: dict[str, str]) -> None:
    """Map each CXL region from the file standing in for its device, by
    region id, instead of holding it in an RM."""
    import mmap

    from maru_handler.client import pool_maps
    from maru_handler.storage.cpu import CpuPool

    def stand_in(info, dax, who):
        path = paths.get(str(dax.handle.region_id))
        if path is None:
            raise OSError(f"no stand-in for region {dax.handle.region_id}")
        fd = os.open(path, os.O_RDWR)
        try:
            mapping = mmap.mmap(
                fd,
                info.capacity,
                flags=mmap.MAP_SHARED,
                prot=mmap.PROT_READ | mmap.PROT_WRITE,
            )
        finally:
            os.close(fd)
        return CpuPool(
            info.pool_id,
            info.capacity,
            info.page_size,
            medium="cxl",
            mapping=mapping,
            shared=True,
        )

    pool_maps.CxlRegionMapping = stand_in


if __name__ == "__main__":
    sys.exit(main())
