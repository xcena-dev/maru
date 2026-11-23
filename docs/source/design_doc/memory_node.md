# Memory node service

`maru-memory-node` owns one DRAM or local CXL store. GPU services use its mutually authenticated TCP endpoint for allocation and publication. Workers issue RDMA READ and WRITE directly through NIXL; the memory node allocates pages, commits completed writes to the Catalog, records reads and evicts by shared LRU.

## Configuration

```json
{
  "node_id": "memory-1",
  "state_dir": "/run/maru-memory-1",
  "server": "tcp://192.0.2.1:5556",
  "page_size": "32MiB",
  "tiers": [{"store": "dram", "size": "16GiB", "hugepage": true}],
  "remote_server": {
    "listen": "192.0.2.10:9871",
    "key_file": "/etc/maru/maru.key",
    "reserve_ttl_s": 60,
    "peer_quarantine_max": "1GiB"
  }
}
```

Use `local_cxl` with `size`, `region_size`, `rm` and `visibility` for a CXL store. The node core owns region claims, holds and restart adoption. Check with `maru-memory-node --config memory.json --check`, then start without `--check`. The command selects the role; JSON rejects `role`. See {doc}`node_storage_service` for settings and validation.

Before starting, configure `UCX_TLS=rc,dc,cuda_copy` and `UCX_RCACHE_ENABLE=n`. Unchecked transports and registration caching are rejected. No NIXL agent is created by `--check`.

## Keys and migration

Both remote configuration objects require `key_file`. The example uses `/etc/maru/maru.key`. Create a key with `openssl rand -hex 32 > maru.key` under `umask 077`, then install the same contents with restricted permissions for GPU services and memory nodes. Worker processes receive registration metadata through their GPU service.

Use the same release for the Catalog, services and workers. The endpoint checks wire version and required capabilities. See {doc}`../getting_started/migrating_to_maru_node` for process and key migration.

## Credit and revocation

Credit belongs to a GPU-node run. Within that run, each vLLM instance uses its own write agent and registration of the same pools. Only ranks of the same instance attached to that run share a write domain; another GPU node or run has a separate domain. All workers share the read registration. Each advertised pool includes its Catalog registration number. A successful WRITE_DONE response follows the Catalog COMMIT result.

FENCE_DOMAIN revokes only the specified domain's write registration before reclaiming its unconfirmed credits. Pages waiting for COMMIT stay owned by their commit tickets. Connection loss, UNKNOWN writes and credit expiry trigger revocation. Elapsed time alone never releases a page.

Failed revocation quarantines only unconfirmed pages. Quarantine counts across all runs of the GPU node. At the configured threshold that GPU node receives no new credit. An explicit successful revocation or a verified memory-node restart permits reclamation. Failed teardown retains mappings and prevents another run in that process.

Up to 64 worker NIXL agents per GPU node are accepted, with at most 1 MiB of metadata each. Up to 64 outstanding write registrations per GPU node are allowed, including registrations whose revocation failed.

## Extensions and memory generation

A memory-node `local_cxl` store can configure `region_extension` for region mapping, retirement, advisory read/write hints and memory-generation checks. HELLO advertises the extension's supported actions; unsupported actions are rejected. A generation change or prolonged unknown generation revokes registrations and invalidates old locations. See {doc}`read_hints` for hooks and defaults.
