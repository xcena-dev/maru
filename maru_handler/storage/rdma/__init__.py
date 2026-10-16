# SPDX-License-Identifier: Apache-2.0
"""RDMA storage: a storage session whose regions live in a memory node's CXL.

The Handler gets regions the way a cxl Handler does: one when the session
opens and one more (``expand``) whenever all of them are full. The region
mapper on a region's memory node maps the region and registers it with the
NIC (``map``), as ``DaxMapper`` maps a region inside a cxl Handler. Bytes move
with one-sided RDMA between a local staging buffer and the region. Other
workers of the namespace read the regions through server leases.

Rules that keep a late RDMA from landing on someone else's data:

* A remote page is reused only after MaruServer removed its key (``remove``),
  and a staging slot only after its transfer completed or NIXL released it.
* A transfer whose completion is not confirmed isolates its pages and slots
  until NIXL releases its handle.
* A region goes back to MaruServer only after its region mapper deregistered
  it (``release``). A WRITE still in the NIC's send queue or in the network
  then fails at the memory node's NIC instead of landing on the region's
  next owner. A region whose release cannot be confirmed stays with the
  session; MaruServer never reuses it without being given it back.

A region whose region mapper cannot map it is set aside: its keys are removed
from MaruServer (so probes stop reporting hits that cannot be read) and new
pages come from the other regions or a new one, until the region maps again.

Modules:
    region: regions, pages and region access.
    datapath: staging buffer, NIXL agent, region mapper connections.
    lifecycle: getting, setting aside and giving back regions; eviction.
    client: the storage client (stores and loads).
    router: several pools behind one worker.
"""

from .client import RdmaStorageClient
from .datapath import RdmaDataPath
from .region import RDMA, RdmaAllocation, RdmaRegion, RegionAccess, pick_mapper
from .router import PoolRouter

__all__ = [
    "RDMA",
    "PoolRouter",
    "RdmaAllocation",
    "RdmaDataPath",
    "RdmaRegion",
    "RdmaStorageClient",
    "RegionAccess",
    "pick_mapper",
]
