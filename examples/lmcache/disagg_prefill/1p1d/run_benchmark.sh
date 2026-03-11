#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

# Source the environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/env.sh" ]; then
    source "$SCRIPT_DIR/env.sh"
fi

vllm bench serve --port $LMCACHE_PROXY_EXTERNAL_PORT --seed $(date +%s) \
    --model Qwen/Qwen2.5-0.5B \
    --dataset-name random --random-input-len 7500 --random-output-len 200 \
    --num-prompts 30 --burstiness 100 --request-rate 1 --ignore-eos