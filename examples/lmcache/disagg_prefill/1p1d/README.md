## Example of Disaggregated Prefill in vLLM v1

This example demonstrates how to run LMCache with disaggregated prefill on a single node using Maru shared storage.

### Prerequisites

- Install [LMCache](https://github.com/LMCache/LMCache). You can simply run `uv pip install lmcache`.
- Install [Maru](https://github.com/xcena-dev/maru). Run `uv pip install -e .` from the maru repo root.
- At least 2 GPUs

### Usage

Optionally set the visible devices for prefill and decoder instances through environment variable.
By default they are set to 0 and 1 respectively.

```bash
export PREFILLER_DEVICE_ID="1"
export DECODER_DEVICE_ID="0"
```

### Port Configuration
You can configure the ports used by the example in [env.sh](env.sh). By default, all ports are calculated based on `LMCACHE_PORT_BASE` (default: 9000 + UID).
If you encounter port conflicts, please change `LMCACHE_PORT_BASE` in [env.sh](env.sh).

#### Running the example

```bash
bash disagg_example_1p1d.sh
```

The script will launch MaruServer, prefiller, decoder, and proxy instances with ports defined in [env.sh](env.sh).

Press `Ctrl+C` to stop all servers.

#### Example benchmark command

If you have vLLM's serving benchmark tool, you can run the following command to benchmark the serving performance of the disaggregated prefill setup (the port is sourced from `LMCACHE_PROXY_EXTERNAL_PORT` in `env.sh`):

```bash
bash run_request.sh
```
or manually:
```bash
source env.sh
vllm bench serve --port $LMCACHE_PROXY_EXTERNAL_PORT --seed $(date +%s) \
    --model Qwen/Qwen2.5-0.5B \
    --dataset-name random --random-input-len 7500 --random-output-len 200 \
    --num-prompts 30 --burstiness 100 --request-rate 1 --ignore-eos
```

Expected output from the benchmark script:

```plaintext
============ Serving Benchmark Result ============
Successful requests:                     30
Failed requests:                         0
Request rate configured (RPS):           1.00
Benchmark duration (s):                  33.41
Total input tokens:                      225000
Total generated tokens:                  5970
Request throughput (req/s):              0.90
Output token throughput (tok/s):         178.69
Peak output token throughput (tok/s):    218.00
Peak concurrent requests:                5.00
Total token throughput (tok/s):          6913.25
---------------Time to First Token----------------
Mean TTFT (ms):                          422.46
Median TTFT (ms):                        422.19
P99 TTFT (ms):                           432.50
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          16.22
Median TPOT (ms):                        16.31
P99 TPOT (ms):                           16.49
---------------Inter-token Latency----------------
Mean ITL (ms):                           16.14
Median ITL (ms):                         15.61
P99 ITL (ms):                            38.92
==================================================
```

### Components

#### Server Scripts
- `disagg_vllm_launcher.sh` - Launches individual vLLM servers for prefill/decode
- `disagg_proxy_server.py` - FastAPI proxy server that coordinates between prefiller and decoder
- `disagg_example_1p1d.sh` - Main script to run the example

#### Configuration
- `configs/maru-prefiller-config.yaml` - Configuration for prefiller server
- `configs/maru-decoder-config.yaml` - Configuration for decoder server

#### Log Files
The main script generates several log files:
- `prefiller.log` - Logs from the prefill server
- `decoder.log` - Logs from the decode server
- `proxy.log` - Logs from the proxy server
