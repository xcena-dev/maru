#!/bin/bash
# Launch a vLLM engine whose KV store is the CXL pool of the memory nodes,
# reached over RDMA (storage_backend "rdma").
#
# Usage:
#   ./rdma_vllm_launcher.sh <inst1|inst2> [model]
#
# Examples:
#   ./rdma_vllm_launcher.sh inst1                          # GPU 0, default model
#   MARU_SERVER_URL=tcp://<metadata node>:<port> MARU_WORKER_UCX_DEVICE=mlx5_0:1 \
#       MARU_INST2_GPU=0 ./rdma_vllm_launcher.sh inst2       # worker on another node
#
# The port is the one maru_server.sh prints on the metadata node.

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

# Each engine opens its own storage session; the name must be unique among
# the engines of one maru-server, also across nodes.
ENGINE_ID=${MARU_ENGINE_ID:-$(hostname -s)-$1}

UCX_ENTRY=""
if [[ -n "$MARU_WORKER_UCX_DEVICE" ]]; then
    UCX_ENTRY="\"maru_rdma_device\": \"${MARU_WORKER_UCX_DEVICE}\","
    # Keep UCX on that NIC only.
    export UCX_NET_DEVICES=${UCX_NET_DEVICES:-$MARU_WORKER_UCX_DEVICE}
fi

# MARU_LOG_TIMING=1 makes the connector log per-request load and store timings
# (and whether the copy kernel or its fallback ran) to stderr.
TIMING_ENTRY=""
if [[ -n "${MARU_LOG_TIMING:-}" ]]; then
    TIMING_ENTRY="\"maru_log_timing\": true,"
fi

NUMA=()
if [[ -n "$MARU_NUMA_NODE" ]]; then
    NUMA=(numactl --cpunodebind="$MARU_NUMA_NODE" --membind="$MARU_NUMA_NODE")
fi

echo "=== Maru-vLLM RDMA KV ==="
echo "  Instance:     $1 (engine id $ENGINE_ID)"
echo "  Model:        $MODEL"
echo "  GPU Device:   $DEVICE"
echo "  vLLM Port:    $PORT"
echo "  maru-server:  $MARU_SERVER_URL"
echo "  UCX device:   ${MARU_WORKER_UCX_DEVICE:-(UCX default)}"
echo "  Region size:  $MARU_POOL_SIZE"
echo "  Namespace:    $MARU_CACHE_NAMESPACE"
echo "  Chunk Tokens: $MARU_KV_CHUNK_TOKENS"
echo "========================="

# The rdma backend uses the storage session contract of the cpu backend:
# synchronous chunkwise loads and stores, --enforce-eager,
# --no-async-scheduling and kv_load_failure_policy "recompute".
CUDA_VISIBLE_DEVICES=$DEVICE exec "${NUMA[@]}" vllm serve "$MODEL" \
    --port "$PORT" \
    --enforce-eager \
    --no-async-scheduling \
    --enable-prompt-tokens-details \
    --max-model-len "$MAX_MODEL_LEN" \
    --gpu-memory-utilization "$GPU_MEM_UTIL" \
    --kv-transfer-config "{
        \"kv_connector\": \"MaruKVConnector\",
        \"kv_connector_module_path\": \"maru_vllm\",
        \"kv_role\": \"kv_both\",
        \"kv_load_failure_policy\": \"recompute\",
        \"kv_connector_extra_config\": {
            \"maru_server_url\": \"${MARU_SERVER_URL}\",
            \"maru_storage_backend\": \"rdma\",
            ${UCX_ENTRY}
            ${TIMING_ENTRY}
            \"maru_pool_size\": \"${MARU_POOL_SIZE}\",
            \"maru_engine_id\": \"${ENGINE_ID}\",
            \"maru_cache_namespace\": \"${MARU_CACHE_NAMESPACE}\",
            \"maru_kv_chunk_tokens\": ${MARU_KV_CHUNK_TOKENS}
        }
    }"
