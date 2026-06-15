# Tools

Three monitors, each at a different layer — they complement rather than
overlap (the only shared figure is pool free):

| Tool | Layer | Source | Answers |
|---|---|---|---|
| `pool_monitor.py` | physical device/pool | resource manager (`:9850`) | "how full is each DAX device?" |
| `usage_monitor.py` | logical per-instance | MaruServer (`:5555`) | "who is holding what, and how much is left?" |
| `stats_monitor.py` | operations/performance | MaruServer (`:5555`) | "who is doing how many ops, how fast?" |

(`maru_rm_tool.py` is a device-header utility, not a monitor.)

## pool_monitor.py

Real-time maru memory pool usage monitor.

```bash
python tools/pool_monitor.py              # one-shot snapshot
python tools/pool_monitor.py -w 1         # top-style refresh every 1s
python tools/pool_monitor.py -w 1 -c 30   # watch 30 times at 1s interval
python tools/pool_monitor.py --csv        # CSV output for post-processing
python tools/pool_monitor.py --scroll     # scrolling log (no screen clear)
```

### Example Output

**TUI mode** (`-w 1`):

```
  Maru Pool Monitor  —  2026-03-11T14:30:05  (Ctrl+C to quit)

  Pool  Type      Used      Free     Total     Delta  Usage
  ----  --------  --------  --------  --------  --------  -----
     0  DEV_DAX     13.8G    229.0G    242.8G            [##----------------------------] 5.7%
     1  DEV_DAX   3319.5G     13.8G   3333.2G   +100.0G  [#############################-] 99.6%
     2  DEV_DAX     13.8G    215.2G    229.0G            [##----------------------------] 6.0%
     3  DEV_DAX       0B     229.0G    229.0G            [------------------------------] 0.0%
```

**CSV mode** (`--csv`):

```
timestamp,pool_id,dax_type,total_bytes,free_bytes,used_bytes,usage_pct
2026-03-11T14:30:05,0,DEV_DAX,260717199360,245861867520,14855331840,5.70
2026-03-11T14:30:05,1,DEV_DAX,3578455826432,14855331840,3563600494592,99.58
```

## usage_monitor.py

Per-instance CXL usage monitor. Shows, for each client instance
(`owner_instance_id`), how much CXL memory it reserved (**allocated**) versus
how much is live KV data (**used**), the difference (**slack**), and the shared
pool free space. Queries MaruServer over the `GET_USAGE` RPC — no `MARU_STAT`
required.

```bash
python -m tools.usage_monitor                  # one-shot snapshot
python -m tools.usage_monitor -w 1             # refresh every 1s
python -m tools.usage_monitor -w 1 -c 30       # 30 iterations then exit
python -m tools.usage_monitor --csv            # CSV for post-processing
python -m tools.usage_monitor --host 10.0.0.1 -p 5556
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | MaruServer host |
| `-p`, `--port` | `5555` | MaruServer port |
| `-w`, `--watch` | `0` | Refresh interval in seconds (0 = one-shot) |
| `-c`, `--count` | `0` | Number of iterations (0 = unlimited) |
| `--csv` | off | CSV output for post-processing |
| `--scroll` | off | Scrolling log instead of top-style refresh |

### Example Output

```
  Maru Usage Monitor  —  2026-06-15T14:30:05  (Ctrl+C to quit)

  owner_instance_id                       regions  allocated       used      slack
  --------------------------------------  -------  ---------  ---------  ---------
  vllm-0                                        3      12.0G       9.4G       2.6G
  vllm-1                                        2       8.0G       7.9G       0.1G
  --------------------------------------  -------  ---------  ---------  ---------
  TOTAL                                         5      20.0G      17.3G       2.7G

  Pool (shared): 229.0G free / 242.8G total  [##----------------------------] 5.7%
```

**CSV mode** (`--csv`):

```
timestamp,instance_id,regions,allocated_bytes,used_bytes,slack_bytes,pool_total_bytes,pool_free_bytes
2026-06-15T14:30:05,vllm-0,3,12884901888,10093173964,2791727924,260717199360,245861867520
```

> **Note:** `instance_id` identifies one `MaruBackend` = one process/worker
> (TP/DP runs one per worker), and the default is a random UUID per process.
> Set `maru_instance_id` in the integration's `extra_config` for stable,
> readable rows.

## stats_monitor.py

Interactive TUI for real-time handler operation metrics — latency, throughput,
hit/miss rates, and per-instance comparison. Polls `GET_STATS` RPC and renders
a curses dashboard with sparkline latency graphs.

### Enable stats collection

Stats reporting is **off by default** (zero overhead). Enable on the client
side via either:

```bash
# Per-run environment variable
export MARU_STAT=1

# Or in MaruConfig
MaruConfig(server_url=..., enable_stats=True)
```

When enabled, each `MaruHandler` spawns a daemon thread that batches stats
into 1-second `REPORT_STATS` RPC flushes.

### Run the monitor

```bash
python -m tools.stats_monitor                      # localhost:5555, 1s refresh
python -m tools.stats_monitor -p 11008             # custom port
python -m tools.stats_monitor --host 10.0.0.1 -p 5556
python -m tools.stats_monitor -i 0.5               # 0.5s refresh
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | MaruServer host |
| `-p`, `--port` | `5555` | MaruServer port |
| `-i`, `--interval` | `1.0` | Refresh interval in seconds |

### Example Output

Interactive curses dashboard (the selected row, `▶`, drives the detail panel):

```
  Maru Stats Monitor  14:30:05    KV:1820 (4.2G)  Alloc:6 (24.0G)  Inst:2

  ▐ ALL ▌   Instance 0    Instance 1

  operation             │   count│  delta│   avg_us│   min_us│   max_us│                  activity
  ──────────────────────┼────────┼───────┼─────────┼─────────┼─────────┼──────────────────────────
▶ store                 │    8124│    +37│     45.2│     12.1│    230.0│         ▂▃▅▇▆▅▃▂▃▄▅▆▇▅▃▂▃▄▅
  retrieve              │   15330│    +71│     18.7│      5.0│     92.4│         ▁▂▄▆▇▆▄▂▁▂▃▅▇▆▄▂▁▂▃
  exists                │   20106│   +112│      6.3│      2.1│     40.0│         ▃▄▅▆▇▇▆▅▄▃▄▅▆▇▆▅▄▃▄▅
  pin                   │   15330│    +71│      7.1│      2.4│     38.9│         ▁▂▃▄▅▆▇▆▅▄▃▂▁▂▃▄▅▆▇▆
  alloc                 │     820│     +4│     12.5│      4.0│     88.0│         ▁▁▂▂▃▃▂▂▁▁▂▂▃▃▂▂▁▁▂▂
  delete                │       0│       │      0.0│      0.0│      0.0│
  batch_retrieve        │    1240│    +6│     210.4│     85.0│    640.0│         ▂▄▆▇▅▃▂▄▆▇▅▃▂▄▆▇▅▃▂▄

  ╭─ store ──────────────────────────────────────────────────────────────────────────────────╮
  │ count: 8124    bytes: 3.8G                                                                  │
  │                                                                                            │
  │ latency (us):  avg=45.2  min=12.1  max=230.0                                                │
  │ throughput:    812.4 MB/s                                                                   │
  ╰────────────────────────────────────────────────────────────────────────────────────────────╯
  ╭─ latency (us) ───────────────────────────────────────────────────────────────────────────╮
  │                                                                                            │
  │  230.0 ┤            ▴         ▴                                                             │
  │        │      ▴   ▴     ▴   ▴                                                               │
  │  115.0 ┤   •    •    •     •      •                                                         │
  │        │ •    •   •    •      •                                                             │
  │        │▾   ▾    ▾    ▾    ▾                                                                 │
  │    0.0 ┤  ▾    ▾                                                                            │
  │        └──────────────────────────────────────                                            │
  │        ▴=max  •=avg  ▾=min                                                                  │
  ╰────────────────────────────────────────────────────────────────────────────────────────────╯

  q:quit  ↑↓:select  ←→:instance
```

(The `delta` column shows the per-interval count; rows with no activity in the
window stay blank. The `activity` sparkline is the last 25 ticks.)

### Key bindings

| Key | Action |
|---|---|
| `↑` / `↓` | Select operation row |
| `←` / `→` | Switch client instance |
| `q` / `Esc` / `Ctrl-C` | Quit |

### Metrics shown

Per `(client_id, op_type)` the server tracks:

- **Cumulative**: count, total bytes, avg/min/max latency (since connect)
- **Interval**: count, avg/min/max latency (reset on each poll — live view)
- **Result classification**: `hit` / `partial` / `miss` for `retrieve`,
  `batch_retrieve`, `exists`, `batch_exists`

Operations covered: `alloc`, `free`, `store`, `retrieve`, `exists`, `pin`,
`unpin`, `delete`, and their `batch_*` variants.

### Overhead

- **Disabled (`MARU_STAT` unset)**: 0 — no thread, no buffer, no instrumentation cost
- **Enabled**: one daemon thread per handler, 1 RPC/sec, bounded batch size
  (`_MAX_STATS_BATCH = 10000` entries per flush)
