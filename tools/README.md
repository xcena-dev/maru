# Tools

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
