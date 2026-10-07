# SPDX-License-Identifier: Apache-2.0
"""Remote CXL storage for Maru: workers use another node's CXL pool over RDMA.

The pool node's maru-server serves remote access (``maru-server
--remote-bind``): it maps remote regions, registers them with NIXL and answers
reservation, publish, lookup and read protection requests from remote
handlers (see ``maru_server.remote_access``). A worker selects the remote
backend with ``MaruConfig(storage_backend="remote")``; its handler then moves
KV bytes between a local staging buffer and the pool's regions with NIXL
READ/WRITE.

Modules:
    protocol: msgpack framing of the control channel.
    transport: NIXL agent wrapper (registration, peers, batched READ/WRITE).
    client: control-channel client used by the worker-side backend.
"""
