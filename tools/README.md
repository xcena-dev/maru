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
