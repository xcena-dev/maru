# Node-local storage

A GPU node serves one store, either `dram` or `local_cxl`. Configure it in `maru-storage` JSON and connect vLLM with `maru_node`.

See [Moving to maru_node](../../../docs/source/getting_started/migrating_to_maru_node.md) for process and setting replacements, and [Node StorageService](../../../docs/source/design_doc/node_storage_service.md) for complete configurations.
