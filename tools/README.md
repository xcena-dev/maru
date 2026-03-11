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
