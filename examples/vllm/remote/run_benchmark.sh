#!/bin/bash
# Run the remote KV sharing test between two vLLM engines.
# Instance 1 stores a prompt's KV in the remote pool; Instance 2 loads it.
#
# Prerequisites:
#   1. The pool node runs remote_pool_server.sh
#   2. Two engines run remote_vllm_launcher.sh (inst1, inst2), on any nodes
#
# Usage:
#   ./run_benchmark.sh [--model MODEL] [--prompt-repeat N] [--max-tokens N]
#
# Engines on other hosts: set MARU_INST1_URL / MARU_INST2_URL (or --url1/--url2).

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python "$SCRIPT_DIR/run_benchmark.py" \
    --url1 "$MARU_INST1_URL" \
    --url2 "$MARU_INST2_URL" \
    "$@"
