# Tools — `marutop`

All maru monitoring and device-admin tooling is exposed through a single
`marutop` command (installed as a console script with the `maru` package, the
way XCENA's `pxl` SDK ships `pxltop`). The implementation lives in the
[`maru_tools`](../maru_tools/) package; the `tools/*.py` files here are thin
backward-compat shims (see [Legacy invocation](#legacy-invocation)).

```bash
marutop                    # unified live view (default) — DEVICES + INSTANCES
marutop pool   [...]       # physical DAX pool gauges
marutop usage  [...]       # per-instance allocated/used/slack
marutop stats  [...]       # per-operation latency/throughput dashboard
marutop device init|show|clear <dax-path>
```

The subcommands sit at three layers and complement rather than overlap (the
only shared figure is pool free):

| Command | Layer | Source | Answers |
|---|---|---|---|
| `marutop` | both (fused) | RM + MaruServer | "one screen: how full, and who holds what?" |
| `marutop pool` | physical device/pool | resource manager (`:9850`) | "how full is each DAX device?" |
| `marutop usage` | logical per-instance | MaruServer (`:5555`) | "who is holding what, and how much is left?" |
| `marutop stats` | operations/performance | MaruServer (`:5555`) | "who is doing how many ops, how fast?" |
| `marutop device` | device header | RM device (via `device_scanner`) | admin: write/show/clear a device's UUID header |

## `marutop` (unified live view)

The default screen is an `htop`-style curses TUI — the analog of `pxltop`'s
DEVICES + PROCESSES layout, with colored gauges (green < 60% < yellow < 85% <
red). It has two screens you switch between:

**Overview** (default):

- **DEVICES** — physical DAX pools from the Resource Manager (`:9850`).
- **INSTANCES** — per-instance allocated/used/slack, with a per-DAX-device gauge
  under each instance (which device its regions landed on, and how much of that
  device it holds). With no `-p`/`--host`, the local `maru-server` processes are
  **auto-discovered** (by scanning `/proc` for their `--port`) and all polled,
  each under a `── server :PORT (pid N) ──` block — so multiple servers are
  covered without knowing their ports. Pass `-p`/`--host` to pin a single
  (possibly remote) server instead.

**STATS view** (select an instance + `enter`): a full per-instance dashboard —
an op table (count / delta / avg / min / max latency + an activity sparkline),
a detail box for the `↑↓`-selected op (hit-rate bar, throughput), and a
min/avg/max latency graph over time. Populated only when clients run with
`MARU_STAT=1` (else a hint). `esc`/`←` returns to the overview. `marutop stats`
remains for a directly port-pinned dashboard.

Backends are polled on a background thread, so the UI stays smooth and keys stay
responsive even when a server is slow or unreachable (its block just shows
`(unavailable: ...)`).

```bash
marutop                    # auto-discover local servers + RM; refresh every 1s
marutop -w 2               # refresh every 2s
marutop -p 11011           # pin one server (e.g. a naru run) — disables discovery
marutop --once             # single plain-text snapshot then exit (scriptable)
marutop --host 10.0.0.1 -p 5556 --address 10.0.0.1:9850
```

Keys — overview: `↑↓` select instance · `enter` open STATS · `s` sort · `i`
interval · `q` quit.  STATS view: `↑↓` select op · `esc`/`←` back · `i`
interval · `q` quit.

## `marutop pool`

Real-time maru memory pool usage.

```bash
marutop pool               # one-shot snapshot
marutop pool -w 1          # top-style refresh every 1s
marutop pool -w 1 -c 30    # watch 30 times at 1s interval
marutop pool --csv         # CSV output for post-processing
marutop pool --scroll      # scrolling log (no screen clear)
```

### Example Output

**TUI mode** (`-w 1`):

```
  Maru Pool Monitor  —  2026-03-11T14:30:05  (Ctrl+C to quit)

  Device            Type          Used      Free     Total     Delta  Usage
  ----------------  --------  --------  --------  --------  --------  -----
  /dev/dax0.0       DEV_DAX      13.8G    229.0G    242.8G            [##----------------------------] 5.7%
  /dev/dax1.0       DEV_DAX    3319.5G     13.8G   3333.2G   +100.0G  [#############################-] 99.6%
```

**CSV mode** (`--csv`):

```
timestamp,dax_path,dax_type,total_bytes,free_bytes,used_bytes,usage_pct
2026-03-11T14:30:05,/dev/dax0.0,DEV_DAX,260717199360,245861867520,14855331840,5.70
```

Options: `-w/--watch`, `-c/--count`, `--csv`, `--scroll`,
`--address` (resource manager `host:port`, default `127.0.0.1:9850`).

## `marutop usage`

Per-instance CXL usage. Shows, for each client instance (`owner_instance_id`),
how much CXL memory it reserved (**allocated**) versus how much is live KV data
(**used**), the difference (**slack**), and the shared pool free space. Queries
MaruServer over the `GET_USAGE` RPC — no `MARU_STAT` required.

```bash
marutop usage                  # one-shot snapshot
marutop usage -w 1             # refresh every 1s
marutop usage -w 1 -c 30       # 30 iterations then exit
marutop usage --csv            # CSV for post-processing
marutop usage --host 10.0.0.1 -p 5556
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

> **Note:** `instance_id` identifies one `MaruBackend` = one process/worker
> (TP/DP runs one per worker), and the default is a random UUID per process.
> Set `maru_instance_id` in the integration's `extra_config` for stable,
> readable rows. Requires a MaruServer that supports the `GET_USAGE` RPC; against
> an older server the tool reports a connection error rather than an empty table.

## `marutop stats`

Interactive TUI for real-time handler operation metrics — latency, throughput,
hit/miss rates, and per-instance comparison. Polls `GET_STATS` RPC and renders
a curses dashboard with sparkline latency graphs.

### Enable stats collection

Stats reporting is **off by default** (zero overhead). Enable on the client
side via either:

```bash
export MARU_STAT=1                              # per-run environment variable
# or in MaruConfig:  MaruConfig(..., enable_stats=True)
```

When enabled, each `MaruHandler` spawns a daemon thread that batches stats
into 1-second `REPORT_STATS` RPC flushes.

### Run the dashboard

```bash
marutop stats                      # localhost:5555, 1s refresh
marutop stats -p 11008             # custom port
marutop stats --host 10.0.0.1 -p 5556
marutop stats -i 0.5               # 0.5s refresh
```

Options: `--host` (default `127.0.0.1`), `-p/--port` (default `5555`),
`-i/--interval` (default `1.0`).

### Key bindings

| Key | Action |
|---|---|
| `↑` / `↓` | Select operation row |
| `←` / `→` | Switch client instance |
| `q` / `Esc` / `Ctrl-C` | Quit |

### Metrics shown

Per `(client_id, op_type)` the server tracks cumulative (count, total bytes,
avg/min/max latency since connect) and interval (reset each poll — live view)
figures, plus `hit`/`partial`/`miss` classification for retrieve/exists ops.
Operations covered: `alloc`, `free`, `store`, `retrieve`, `exists`, `pin`,
`unpin`, `delete`, and their `batch_*` variants.

## `marutop device`

Write/show/clear the UUID header the Resource Manager uses to identify a
DEV_DAX device across reboots.

```bash
marutop device init /dev/dax0.0          # write UUID header
marutop device init --show /dev/dax0.0   # show existing UUID (alias of `show`)
marutop device init --force /dev/dax0.0  # force regenerate UUID
marutop device show /dev/dax0.0          # show existing UUID
marutop device clear /dev/dax0.0         # clear header (zero-fill)
marutop device clear --yes /dev/dax0.0   # clear without prompt
```

## Legacy invocation

The old module invocations still work from the repo root via thin shims that
forward to `maru_tools`:

```bash
python -m tools.pool_monitor -w 1     # → marutop pool -w 1
python -m tools.usage_monitor --csv   # → marutop usage --csv
python -m tools.stats_monitor -i 0.5  # → marutop stats -i 0.5
python tools/maru_rm_tool.py device init /dev/dax0.0   # → marutop device init ...
```

Prefer `marutop` in new scripts and docs.
