# Read and Write Hints

Some CXL memory answers every address but not always at the same speed. A
CXL-SSD, for example, keeps a DRAM cache in front of its flash: an RDMA READ of
a page already in DRAM runs at DRAM speed, one of a page still on flash waits
inside the device until the page is brought in. An RDMA WRITE to a page still
on flash makes the device bring that page in first, page by page. The device
writes data back to flash on its own. Such a device is correct without any help
from Maru, but it is fast only when it knows early which pages are coming.

Maru knows that early. A request waits for the running engine step before the
scheduler picks it, and for the scheduler to admit its load before the worker
starts it. A **read hint** carries the pages a request will read to the memory
side during that wait, so the device can prefetch them. A **write hint** names
the pages a store is about to write, so the device can prepare them instead of
filling each from flash on its first WRITE. Maru's KV data path (lookup, lease,
RDMA READ and WRITE) is the same with or without hints.

## Who owns what

Grey boxes are Maru; the orange box is a separately installed package. Thick
arrows carry KV bytes, thin arrows carry control messages.

```mermaid
flowchart LR
  subgraph gpu["GPU node"]
    sch["Maru scheduler<br/>(hint thread)"]
    wrk["Maru worker"]
  end
  subgraph mem["memory node"]
    ms["maru-server"]
    ag["remote CXL agent<br/>(checks every range)"]
    ext["region extension<br/>(maru.region_extensions)"]
    dev[("CXL-SSD<br/>DRAM cache + flash")]
  end
  sch -->|"probe locations (no lease)"| ms
  sch -->|"read_hint / read_hint_end"| ag
  ag -->|"checked hint"| ext
  ag -->|"map (fresh)"| ext
  ext -.->|"prefetch, discard"| dev
  wrk -->|"write_hint"| ag
  wrk -->|"lookup, lease"| ms
  wrk ==>|"RDMA READ / WRITE"| dev
  classDef maru fill:#e8e8e8,stroke:#555,color:#111
  classDef pkg fill:#fbe3cc,stroke:#c06000,color:#111
  class sch,wrk,ms,ag maru
  class ext pkg
```

- **Maru** decides which keys hit, which pages hold them and which bytes move.
  It finds where a request's hits are, sends the hint to the remote CXL agent
  of their regions and never waits for an answer.
- **The remote CXL agent** maps and registers regions. It hands a hint to its
  region extension only after checking that every range lies in a region it
  maps and some connection holds, under that region's own capability. The
  sender needs to hold no region itself: the scheduler holds none.
- **The region extension** (entry point group `maru.region_extensions`,
  selected with `--region-extension`) owns whatever it does to the device. It
  hears each region as it is mapped, with `fresh` when the region's owner says
  it has just allocated it and written nothing to it yet, so stale data on
  flash need not be read back for the first writes. `fresh` is the owner's
  word, not a proof, so the extension uses it only to drop the region's own
  bytes, which the owner could overwrite anyway. It may refuse to map a region
  and keep a region from going back while a device command on it still runs.
  The agent advertises in its hello which hint actions the extension takes;
  Maru sends hints only to agents that advertise them.
- **Latency.** The agent asks the extension whether a region may go back while
  every other request waits, and hands it each hint and asks it the memory
  generation on request threads. The extension answers these from its own
  state within microseconds: it queues a hint for its own thread and never
  waits on the device or another process there.

## One request

```mermaid
sequenceDiagram
  participant E as vLLM engine
  participant S as Maru scheduler
  participant A as agent + region extension
  participant D as CXL-SSD
  participant W as Maru worker
  E->>S: request received (hint on arrival, opt-in)
  S->>S: probe locations at maru-server
  S-)A: read_hint(request, ranges in read order)
  A-)D: prefetch into DRAM
  Note over E: request waits for the running step
  E->>W: load the request's hits
  W-)A: read_promote(request)
  A-)D: this request's bytes first
  W->>D: RDMA READ (an early one waits in the device)
  W->>E: load done
  E->>S: load done
  S-)A: read_hint_end(request)
  Note over W: the request decodes; its new KV is stored
  W->>A: write_wait(store, pages in write order)
  A-->>W: pages ready (or the wait ran out)
  W->>D: RDMA WRITE
```

- **Where hints come from.** With `maru_read_hints` and `maru_async_load`, the
  vLLM scheduler connector hints a request the first time it finds a load for
  it, or, with `maru_read_hint_on_arrival`, as soon as vLLM's engine receives
  it. Each load is hinted once and its hint is ended once, as soon as vLLM
  reports the load done, so the memory side stops keeping the hinted bytes
  while the request decodes; a request that finishes first ends it then. A
  process that names no request to its storage (LMCache's `maru://` backend,
  for example) can set `read_hints` on its worker instead: each load is then
  hinted as its READs start.
- **Read promotes.** Many hinted requests can wait at once, and the memory
  side brings their bytes in in hint order, which is not the order their
  loads run in. When a worker with `read_promotes` (in vLLM, on with
  `maru_read_hints` and `maru_async_load`) starts the load of a request, it
  sends `read_promote` naming the request, so the memory side brings that
  request's bytes in ahead of the others. The worker did not send the hint
  (its scheduler did), so it sends the promote to every agent of the pool
  that takes it, and the extension matches it to the hint by request id.
  It is queued like a hint, has no ranges and no end.
- **Write hints.** A worker with `write_hints` (in vLLM, `maru_write_hints`,
  which follows `maru_read_hints` unless set) sends a write hint for each
  store once the store has its pages and before it posts the WRITEs. It is
  queued like a read hint and has no end. With vLLM the store takes its pages
  only on the thread that writes them, so a queued hint leads the WRITEs only
  by the time the hint thread takes to send it. So when the agent's region
  extension takes `write_wait`, the store sends its pages in a `write_wait`
  instead and waits for the answer before it posts the WRITEs: at most
  `write_wait_ms` (50 ms by default, `maru_write_wait_ms`; 0 never waits),
  plus the reply timeout if the agent does not answer. The WRITEs go
  whatever the answer, and stores run off the request path (vLLM's
  write-behind thread), so a wait costs no TTFT. The memory side may only
  prepare the pages, never change what they hold: a hint can come late or be
  wrong. An agent gets write hints and waits only if its region extension
  says it takes them; a wait holds only the store's own connection.
- **Hint on arrival hooks into vLLM.** vLLM offers no interface for this, so
  Maru wraps an internal method of vLLM's engine. It does so only if the
  method has the shape Maru expects; otherwise hints on arrival are off with
  a warning, and the scheduler still hints when it first finds the load. A
  vLLM upgrade can change that method, so check this warning after one.
- **No waiting.** The caller only queues the hint. One thread per backend looks
  the keys up and sends the messages in the order hints were given. While 1024
  hints wait, further hints are dropped with a warning; ends are never dropped.
  A process keeps at most 4096 hinted requests open and ends the oldest past
  that; the agent refuses a connection's hints past its own limit (4096 by
  default), and the extension bounds what it keeps as well.
- **Advice only.** A hint that is late, lost or wrong costs only the time it
  would have saved: the load still leases and reads every key. A hint on
  arrival may name more chunks than the load reads, since vLLM's own prefix
  cache is consulted later. A process that dies sends no end, so the extension
  also expires what it prepared; the agent also tells it when any connection
  closes, so it can end what that connection's hints started.

The message formats and the extension's hooks, locks and failure outcomes are
documented in `maru_handler/storage/remote_cxl/hints.py`,
`maru_remote/region_map.py` and `maru_remote_cxl_agent/extension.py`.

## Memory resets

A write-back cache loses what it held when the device resets, even if the
node keeps running. Maru already handles the same loss after a reboot of the
memory node: the agent advertises the node's boot, and when a worker sees it
change, the owner gives its regions back (their keys go) and a reader whose
region was written under another boot loads a miss.

A region extension can tell the memory's generation, which changes on every
reset that may have lost data. The agent then advertises the node's boot and
that generation together as its boot. When the generation changes while the
agent runs, the agent deregisters every region from the NIC, so RDMA still
under way or still to come through an old registration fails, and drops its
mappings; hello and map replies carry the new boot from then on, and a map
that was under way when the memory was reset fails. The owners' and readers'
existing reboot handling does the rest.

- **How soon.** The agent reads the generation every 100 ms on its own thread
  (`--memory-generation-poll-s`), and on every hello, map and sweep, so a
  reset is acted on within the extension's own detection time plus that
  interval. RDMA that lands in that window is lost with the reset, as any
  write the device had not written back.
- **When the extension does not answer.** For a short while (2 s by default,
  `--memory-generation-grace-s`) the agent keeps the generation it had. After
  that the generation is unknown: every region goes as on a reset, the boot
  says `unknown` and the agent maps nothing until the extension answers
  again. The boot that follows is always new (it also counts these periods),
  even if the generation itself did not change: a reset while the extension
  could not tell would not show in it, so a region mapped before an unknown
  period is never trusted after it.

## Scope

Read hints are sent on the `remote_cxl` backend. A CXL-SSD attached to the GPU
node's own host has the same need; there no agent sits between Maru and the
device, and hints would reach the device software through a handler plugin.
