# Remote node storage

Use a GPU-host `maru-storage` with `remote`, a `maru-memory-node` serving DRAM or local CXL, and one `maru-node-server` Catalog. Workers issue RDMA READ and WRITE directly through their own registered staging.

See [Moving to maru_node](../../../docs/source/getting_started/migrating_to_maru_node.md) for process and setting replacements, and [Node StorageService](../../../docs/source/design_doc/node_storage_service.md) for complete configurations and staging budgets.
