# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Multi-storage admin CLI.

    python -m maru_server.ms_admin delete --server tcp://HOST:PORT \\
        --namespace NS KEY [KEY ...]
"""

import argparse
import sys

import zmq

from maru_common.protocol import AdminDeleteRequest, MessageType
from maru_common.serializer import Serializer
from maru_common.storage_types import MAX_BATCH, Key, Status


def delete(server: str, keys: list[Key], timeout_ms: int = 5000) -> list[str]:
    """ADMIN_DELETE in batches; returns one result line per key."""
    serializer = Serializer()
    context = zmq.Context()
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.connect(server)
    lines = []
    try:
        for start in range(0, len(keys), MAX_BATCH):
            batch = keys[start : start + MAX_BATCH]
            sock.send(
                serializer.encode(
                    MessageType.ADMIN_DELETE, AdminDeleteRequest(items=batch)
                )
            )
            _, response = serializer.decode_response(sock.recv())
            if response.status != Status.OK:
                raise RuntimeError(f"{response.status}: {response.error}")
            for k, item in zip(batch, response.items, strict=True):
                reason = f" ({item.reason})" if item.reason else ""
                lines.append(f"{k.key}\t{item.result}{reason}")
    finally:
        sock.close()
        context.term()
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Multi-storage admin")
    sub = parser.add_subparsers(dest="command", required=True)
    remove = sub.add_parser("delete", help="remove keys (their nodes retire them)")
    remove.add_argument("--server", required=True, help="MaruServer, tcp://HOST:PORT")
    remove.add_argument("--namespace", required=True)
    remove.add_argument("--timeout-ms", type=int, default=5000)
    remove.add_argument("keys", nargs="+")
    args = parser.parse_args(argv)
    keys = [Key(namespace=args.namespace, key=k) for k in args.keys]
    try:
        lines = delete(args.server, keys, args.timeout_ms)
    except (RuntimeError, zmq.ZMQError) as e:
        print(f"delete failed: {e}", file=sys.stderr)
        return 1
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
