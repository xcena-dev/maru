# Multi-Turn Staging

In a multi-turn conversation, every turn re-reads the prefix the previous turns
built. When that prefix lives on an SSD-backed CXL device, the read lands inside
the arriving request's time-to-first-token.

Multi-turn staging moves the read off that path. When a turn finishes, the
connector already knows the exact keys the session's next turn will re-read — it
just stored them — so it asks the device to bring those bytes into device DRAM
during the user's think time. The next turn then reads from DRAM.

The measured result on an XCENA InfiniteMemory device: a 16 GiB device DRAM
cache served 48 concurrent LMSYS conversations at the same response speed as a
128 GiB cache, while holding 33.6 GiB of stored KV — 2.1x more than the cache.

## Turning it on

```yaml
environment:
  MARU_PLUGINS: gaia
  MARU_GAIA_DEVICE_ID: '0'
  MARU_STAGE_PIPELINE: '1'
```

That is the whole switch. Everything that decides *how* staging behaves is
either derived from the model or set to the configuration these numbers were
measured with.

The client must send a session id so turns of one conversation can be linked.
With an OpenAI-compatible client that is `extra_body`:

```python
client.chat.completions.create(
    model=model,
    messages=messages,
    extra_body={"kv_transfer_params": {"maru_session_id": session_id}},
)
```

Send `"maru_session_end": true` on the conversation's last turn so its staging
slot is returned instead of waiting for a turn that never arrives.

## What the code decides for you

Three quantities used to be environment variables. They are not choices — they
follow from the KV geometry the engine already reports, so the connector reads
them itself.

| Quantity | Where it comes from |
|---|---|
| Bytes in one fill command | The size of one stored KV object. Contiguous objects are merged into one device range and then split back on object boundaries. |
| Byte budget scale | The same object size. The admission window's capacity cap (`MARU_STAGE_MAX_BYTES`) is counted in keys, so the scale must be the real object size or the cap means a different amount than it says. |
| Fill worker count | The admission window size, since the scheduler never submits more plans than the window admits. |

One object holds `maru_kv_chunk_tokens` tokens of KV for every layer. On
Llama-3.1-8B with 128-token chunks that is 16 MiB:

```
32 layers x 2 (K,V) x 8 kv_heads x 128 head_dim x 2 B (bf16) = 128 KiB per token
128 tokens x 128 KiB                                        = 16 MiB per object
```

Change `maru_kv_chunk_tokens`, or run a different model, and all three
quantities follow. Nothing in the configuration needs editing.

Each run reports what it resolved, on the timing channel:

```
Maru timing: stage init: enabled=True trigger=turn_end policy=fifo
  window=18 kv_object_bytes=16777216 release=store
maru INFO: gaia_stage_pin summary: ... object_bytes=16777216 ...
```

## What you choose

| Knob | Default | What it decides |
|---|---|---|
| `MARU_STAGE_PIPELINE` | `0` | Whether staging runs at all. |
| `MARU_GAIA_DRAM_SIZE` | — | Device DRAM cache size in GiB (naru sets it on the device). The axis the experiment varies. |
| `MARU_STAGE_MAX_REQUESTS` | `18` | How many sessions' prefixes may occupy device DRAM at once. |
| `MARU_STAGE_MAX_BYTES` | `10 GiB` | The same limit expressed in bytes. `0` removes it. |
| `MARU_GAIA_STAGE_PIN` | `0` | Mark staged bytes as not-evictable until consumed. Off in the measured configuration — see below. |

The remaining `MARU_STAGE_*` and `MARU_GAIA_STAGE_*` knobs exist for
experiments that explore alternatives (a deadline-ordered admission policy,
pacing between fills, an arrival-time trigger). Their defaults are what the
measured configuration used, so leave them unset.

### Why pinning is off

Pinning sounds like the safe choice: it keeps a staged prefix in DRAM until its
turn consumes it. Measured, it made things worse. `memory_pin` runs a fill
inside the call, so pinning a whole prefix in one command put a multi-hundred
millisecond device operation in the way of the GPU-to-CXL write-behind that
carries the *current* request's first token. Five of ten measured cells died
that way. The eviction pressure that pinning was meant to prevent turns out not
to bite at these window sizes, so the default is off.

## Reading a run

Two counters say whether staging did its job:

- **Consume wait** — how long an arriving turn waited for its prefix. In the
  measured configuration this is 0.0 ms at p50 and p90: the fill had finished a
  median 1.27 s before the turn arrived.
- **Slot occupancy** — `stage slot retire: ... window=N/18` lines show how full
  the admission window was. Sustained `18/18` means sessions are queueing for
  space, which shows up as later fills rather than slower reads.

A run where staging is working but not helping usually has plenty of DRAM: if
the whole working set already fits in the device cache, there is nothing to
bring in. Shrink `MARU_GAIA_DRAM_SIZE` until the cache is smaller than the
stored KV before concluding anything about staging.
