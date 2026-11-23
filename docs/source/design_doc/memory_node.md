# Memory node service

`maru-memory-node` owns one DRAM or local CXL store. GPU nodes use its
mutually authenticated TCP endpoint for allocation and publication. Workers
transfer bytes directly through NIXL; the service allocates pages, commits
completed writes to the Catalog, records reads and evicts by LRU.

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

Use `local_cxl` with `size`, `region_size`, `rm` and `visibility` for a local
CXL store. The node core owns region claims, leases and restart adoption.
Check configuration with `maru-memory-node --config memory.json --check`,
then start with `maru-memory-node --config memory.json`.

Before starting, configure `UCX_TLS=rc,dc,cuda_copy` and
`UCX_RCACHE_ENABLE=n`. Unchecked transports and registration caching are
rejected. No NIXL agent is created by `--check`.

## Keys and migration

The shared deployment-key default is `/etc/maru/maru.key`. Create a key with
`openssl rand -hex 32 > /etc/maru/maru.key` and `chmod 600 /etc/maru/maru.key`.
Install the same key on the GPU services and memory nodes, and set
`remote_server.key_file` explicitly. Worker processes receive registration
metadata through their GPU service.

Existing remote CXL agent key files can be copied to the configured path;
the key bytes do not need to change. Existing per-user remote CXL key copies
retain their paths. The memory-node endpoint requires `remote_v1` and
`worker_rdma_v1`, accepts the current remote wire version, and rejects
retired data-staging and drop messages.

## Credit and revocation

Credit belongs to a GPU node run. Every vLLM instance uses its own write
agent and registration of the same pools; all workers share the read
registration. Each advertised pool includes its Catalog registration number.
A successful WRITE_DONE response follows the Catalog COMMIT result.

FENCE_DOMAIN revokes the instance's write registration before reclaiming
its unconfirmed credits. Pages waiting for COMMIT stay owned by their
commit tickets. Connection loss, UNKNOWN writes and credit expiry trigger
revocation. Elapsed time alone never releases a page.

Failed revocation quarantines only unconfirmed pages. Quarantine counts
across all runs of the GPU node. At the configured threshold that GPU node
receives no new credit. An explicit successful revocation or a verified
memory-node restart permits reclamation. Failed teardown retains mappings
and prevents another run in that process.

AGENTS accepts up to 64 worker agents per GPU node, with at most 1 MiB of
metadata per agent. Up to 64 outstanding write agents per GPU node are
allowed, including agents whose revocation failed. HINT returns an
unsupported-action response and HELLO advertises no hint actions.
