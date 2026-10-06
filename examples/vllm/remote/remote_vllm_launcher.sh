#!/bin/bash
# Launch a vLLM engine whose KV store is the remote CXL pool.
#
# Usage:
#   ./remote_vllm_launcher.sh <inst1|inst2> [model]
#
# Examples:
#   ./remote_vllm_launcher.sh inst1                      # GPU 0, default model
#   MARU_REMOTE_URL=tcp://<pool node>:<remote port> MARU_WORKER_UCX_DEVICE=mlx5_0:1 \
#       MARU_INST2_GPU=0 ./remote_vllm_launcher.sh inst2   # worker on another node
#
# The remote port is the one remote_pool_server.sh prints on the pool node.

set -euo pipefail

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <inst1|inst2> [model]"
    exit 1
fi

MODEL="${2:-${MODEL:-Qwen/Qwen2.5-0.5B}}"

if [[ $1 == "inst1" ]]; then
    DEVICE=$MARU_INST1_GPU
    PORT=$MARU_INST1_PORT
elif [[ $1 == "inst2" ]]; then
    DEVICE=$MARU_INST2_GPU
    PORT=$MARU_INST2_PORT
else
    echo "Invalid role: $1 (expected inst1 or inst2)"
    exit 1
fi

UCX_ENTRY=""
if [[ -n "$MARU_WORKER_UCX_DEVICE" ]]; then
    UCX_ENTRY="\"maru_remote_ucx_device\": \"${MARU_WORKER_UCX_DEVICE}\","
fi

echo "=== Maru-vLLM remote KV ==="
echo "  Instance:     $1"
echo "  Model:        $MODEL"
echo "  GPU Device:   $DEVICE"
echo "  vLLM Port:    $PORT"
echo "  Pool:         $MARU_REMOTE_URL"
echo "  UCX device:   ${MARU_WORKER_UCX_DEVICE:-(UCX default)}"
echo "  Namespace:    $MARU_CACHE_NAMESPACE"
echo "  Chunk Tokens: $MARU_KV_CHUNK_TOKENS"
echo "==========================="

# The remote backend needs eager mode and recompute-on-failure; async load and
# store keep the engine stepping while KV moves over RDMA.
KV_CONFIG=$(cat <<EOJSON
{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_load_failure_policy": "recompute",
    "kv_connector_extra_config": {
        "maru_storage_backend": "remote",
        "maru_remote_url": "${MARU_REMOTE_URL}",
        ${UCX_ENTRY}
        "maru_cache_namespace": "${MARU_CACHE_NAMESPACE}",
        "maru_kv_chunk_tokens": ${MARU_KV_CHUNK_TOKENS},
        "maru_async_load": true,
        "maru_async_store": true
    }
}
EOJSON
)

CUDA_VISIBLE_DEVICES=$DEVICE \
    vllm serve "$MODEL" \
    --enforce-eager \
    --gpu-memory-utilization "$GPU_MEM_UTIL" \
    --max-model-len "$MAX_MODEL_LEN" \
    --port "$PORT" \
    --enable-prompt-tokens-details \
    --kv-transfer-config "$KV_CONFIG"
