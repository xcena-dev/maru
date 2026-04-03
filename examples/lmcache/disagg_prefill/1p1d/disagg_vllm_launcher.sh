#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source the environment variables
if [ -f "$SCRIPT_DIR/env.sh" ]; then
    source "$SCRIPT_DIR/env.sh"
fi

# NOTE: For correct KV cache transfer, ensure all processes use the same PYTHONHASHSEED to keep the hash of the KV cache consistent across processes.
export PYTHONHASHSEED=0

# Function to resolve environment variables in config file
resolve_config() {
    local config_file=$1
    local resolved_file="/tmp/$(basename $config_file .yaml)-resolved-$$.yaml"
    envsubst < "$config_file" > "$resolved_file"
    echo "$resolved_file"
}

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <prefiller | decoder> [model]"
    exit 1
fi

if [[ $# -eq 1 ]]; then
    echo "Using default model: Qwen/Qwen2.5-0.5B"
    MODEL="Qwen/Qwen2.5-0.5B"
else
    echo "Using model: $2"
    MODEL=$2
fi


if [[ $1 == "prefiller" ]]; then
    prefill_config_file=$SCRIPT_DIR/configs/maru-prefiller-config.yaml
    echo "Using Maru prefiller config: $prefill_config_file"

    # Resolve environment variables in config file
    resolved_config=$(resolve_config "$prefill_config_file")
    echo "Resolved config: $resolved_config"

    UCX_TLS=cuda_ipc,cuda_copy,tcp \
        LMCACHE_CONFIG_FILE=$resolved_config \
        VLLM_ENABLE_V1_MULTIPROCESSING=1 \
        VLLM_WORKER_MULTIPROC_METHOD=spawn \
        CUDA_VISIBLE_DEVICES=${PREFILLER_DEVICE_ID:-0} \
        vllm serve $MODEL \
        --gpu-memory-utilization ${GPU_MEM_UTIL:-0.9} \
        --port $LMCACHE_PREFILLER_PORT \
        --enforce-eager \
        --no-enable-prefix-caching \
        --kv-transfer-config \
        '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_producer","kv_connector_extra_config": {"discard_partial_chunks": false, "lmcache_rpc_port": "producer1"}}'




elif [[ $1 == "decoder" ]]; then
    decode_config_file=$SCRIPT_DIR/configs/maru-decoder-config.yaml
    echo "Using Maru decoder config: $decode_config_file"

    # Resolve environment variables in config file
    resolved_config=$(resolve_config "$decode_config_file")
    echo "Resolved config: $resolved_config"

    UCX_TLS=cuda_ipc,cuda_copy,tcp \
        LMCACHE_CONFIG_FILE=$resolved_config \
        VLLM_ENABLE_V1_MULTIPROCESSING=1 \
        VLLM_WORKER_MULTIPROC_METHOD=spawn \
        CUDA_VISIBLE_DEVICES=${DECODER_DEVICE_ID:-1} \
        vllm serve $MODEL \
        --gpu-memory-utilization ${GPU_MEM_UTIL:-0.9} \
        --port $LMCACHE_DECODER_PORT \
        --enforce-eager \
        --no-enable-prefix-caching \
        --kv-transfer-config \
        '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_consumer","kv_connector_extra_config": {"discard_partial_chunks": false, "lmcache_rpc_port": "consumer1", "skip_last_n_tokens": 1}}'


else
    echo "Invalid role: $1"
    echo "Should be either prefill, decode"
    exit 1
fi
