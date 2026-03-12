#!/bin/bash
# Run P2P KV cache sharing test between two vLLM instances
#
# Prerequisites:
#   1. maru-server running
#   2. Two vLLM instances running via launch_vllm.sh
#
# Usage:
#   ./run_test.sh [--model MODEL] [--wait-time SEC]

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python "$SCRIPT_DIR/run_request.py" \
    --port1 "$MARU_INST1_PORT" \
    --port2 "$MARU_INST2_PORT" \
    "$@"
