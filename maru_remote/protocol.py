# SPDX-License-Identifier: Apache-2.0
"""Request/response framing for the remote-storage control channel.

Every message is one msgpack map with an ``op`` field. ZMQ frames the bytes,
so no length prefix is needed.
"""

from __future__ import annotations

from typing import Any

import msgpack

PROTOCOL_VERSION = 1


def encode(op: str, **fields: Any) -> bytes:
    """Serialize a request or response with its ``op`` name.

    Args:
        op: Operation name (``"lookup"``, ``"reserve"``, ...).
        **fields: Payload fields; ``bytes`` values are kept as binary.

    Returns:
        msgpack bytes.
    """
    return bytes(msgpack.packb({"op": op, **fields}, use_bin_type=True))


def decode(data: bytes) -> dict[str, Any]:
    """Parse one message and validate its shape.

    Args:
        data: msgpack bytes produced by :func:`encode`.

    Returns:
        The decoded map, including ``op``.

    Raises:
        ValueError: if the bytes are not a map with a string ``op``.
    """
    try:
        obj = msgpack.unpackb(data, raw=False)
    except Exception as exc:  # msgpack raises several unrelated types
        raise ValueError(f"malformed remote-storage message: {exc}") from exc
    if not isinstance(obj, dict) or not isinstance(obj.get("op"), str):
        raise ValueError("remote-storage message must be a map with a string 'op'")
    return obj
