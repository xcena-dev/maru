# SPDX-License-Identifier: Apache-2.0
"""RDMA access to CXL memory on another node.

Workers that are not attached to CXL keep their storage-session regions in a
memory node's CXL and move KV bytes with one-sided RDMA. The remote CXL agent on
each memory node maps a region and registers it with the NIC when a Handler
asks it to (``map``), and deregisters it when the owner gives it back
(``release``).

Modules:
    region_map: Handler side of the remote CXL agent protocol.
    transport: NIXL agent wrapper (registration, peers, batched READ/WRITE).
"""
