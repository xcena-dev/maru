# SPDX-License-Identifier: Apache-2.0
"""Remote CXL storage for Maru: a pool node exports its CXL regions over RDMA.

A pool node runs ``maru-remote-server`` next to its MaruServer and resource
manager. The server owns a ``MaruHandler`` on the node's CXL device, registers
the mapped regions with NIXL and answers reservation, publish, lookup and read
protection requests from remote handlers. A worker selects the remote backend
with ``MaruConfig(storage_backend="remote")``; its handler then moves KV bytes
between a local staging buffer and the pool's regions with NIXL READ/WRITE.

Modules:
    protocol: msgpack framing of the control channel.
    transport: NIXL agent wrapper (registration, peers, batched READ/WRITE).
    client: control-channel client used by the worker-side backend.
    server: the pool-node server and its ZMQ loop.
"""
