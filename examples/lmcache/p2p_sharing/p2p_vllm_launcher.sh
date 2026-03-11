#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# NOTE: For correct KV cache transfer, ensure all processes use the same PYTHONHASHSEED to keep the hash of the KV cache consistent across processes.

# Function to resolve environment variables in config file
resolve_config() {
    local config_file=$1
    local resolved_file="/tmp/$(basename $config_file .yaml)-resolved-$$.yaml"
    envsubst < "$config_file" > "$resolved_file"
    echo "$resolved_file"
}

GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.1}"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <inst1 | inst2> [model]"
    exit 1
fi

if [[ $# -eq 1 ]]; then
    MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
    echo "Using model: ${MODEL}"
else
    echo "Using model: $2"
    MODEL=$2
fi


resolved_config=$(resolve_config "$SCRIPT_DIR/configs/maru-config.yaml")
echo "Resolved config: $resolved_config"

if [[ $1 == "inst1" ]]; then
    DEVICE=0
    PORT=$LMCACHE_INST1_PORT
elif [[ $1 == "inst2" ]]; then
    DEVICE=1
    PORT=$LMCACHE_INST2_PORT
else
    echo "Invalid role: $1"
    echo "Should be either inst1, inst2"
    exit 1
fi

PYTHONHASHSEED=123 \
    UCX_TLS=cuda_ipc,cuda_copy,tcp \
    CUDA_VISIBLE_DEVICES=$DEVICE \
    LMCACHE_CONFIG_FILE=$resolved_config \
    vllm serve $MODEL \
    --gpu-memory-utilization $GPU_MEM_UTIL \
    --port $PORT \
    --no-enable-prefix-caching \
    --kv-transfer-config \
    '{"kv_connector":"LMCacheConnectorV1", "kv_role":"kv_both"}'
