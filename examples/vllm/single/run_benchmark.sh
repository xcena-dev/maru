#!/bin/bash
# Run the single-instance store→reuse benchmark (one instance, cold vs warm).
# Sends the same prompt twice to the same vLLM instance and measures TTFT to
# validate that KV is stored to and loaded back from Maru (cache hit).
#
# Prerequisites:
#   1. maru-server running
#   2. One vLLM instance running via single_vllm_launcher.sh
#
# Usage:
#   ./run_benchmark.sh [--model MODEL] [--max-tokens N] [--repeat-count N]

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Both ports point at the single instance: 1st call stores, 2nd call reuses.
python "$SCRIPT_DIR/run_benchmark.py" \
    --port1 "$MARU_INST_PORT" \
    --port2 "$MARU_INST_PORT" \
    "$@"
