#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to resolve environment variables in config file
resolve_config() {
    local config_file=$1
    local resolved_file="/tmp/$(basename $config_file .yaml)-resolved-$$.yaml"
    envsubst < "$config_file" > "$resolved_file"
    echo "$resolved_file"
}

GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.1}"
DEVICE="${CUDA_DEVICE:-0}"

if [[ $# -ge 1 ]]; then
    MODEL="$1"
else
    MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
fi
echo "Using model: ${MODEL}"

resolved_config=$(resolve_config "$SCRIPT_DIR/configs/maru-config.yaml")
echo "Resolved config: $resolved_config"

PYTHONHASHSEED=123 \
    CUDA_VISIBLE_DEVICES=$DEVICE \
    LMCACHE_CONFIG_FILE=$resolved_config \
    vllm serve $MODEL \
    --gpu-memory-utilization $GPU_MEM_UTIL \
    --port $LMCACHE_INST_PORT \
    --no-enable-prefix-caching \
    --kv-transfer-config \
    '{"kv_connector":"LMCacheConnectorV1", "kv_role":"kv_both"}'
