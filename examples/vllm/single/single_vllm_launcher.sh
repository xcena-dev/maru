#!/bin/bash
# Launch ONE vLLM instance with the Maru KV connector (direct, no LMCache).
#
# vLLM's own prefix cache is disabled so that Maru is the ONLY cache source:
# a repeated prompt must then be served from Maru, which exercises the
# connector's store and load paths (the point of the single-instance check).
#
# Usage:
#   ./single_vllm_launcher.sh [model]

set -euo pipefail

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.1}"
DEVICE="${CUDA_DEVICE:-0}"
MODEL="${1:-${MODEL:-Qwen/Qwen2.5-0.5B}}"
echo "Using model: ${MODEL}"

echo "=== Maru-vLLM Direct Integration (single) ==="
echo "  GPU Device:   $DEVICE"
echo "  vLLM Port:    $MARU_INST_PORT"
echo "  Maru Server:  $MARU_SERVER_URL"
echo "  Pool Size:    $MARU_POOL_SIZE"
echo "  Chunk Tokens: $MARU_KV_CHUNK_TOKENS"
echo "  Prefix cache: OFF (Maru is the only cache source)"
echo "=============================================="

# Build kv-transfer-config JSON
KV_CONFIG=$(cat <<EOJSON
{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "maru_server_url": "${MARU_SERVER_URL}",
        "maru_pool_size": "${MARU_POOL_SIZE}",
        "maru_kv_chunk_tokens": ${MARU_KV_CHUNK_TOKENS}
    }
}
EOJSON
)

CUDA_VISIBLE_DEVICES=$DEVICE \
    vllm serve "$MODEL" \
    --gpu-memory-utilization "$GPU_MEM_UTIL" \
    --port "$MARU_INST_PORT" \
    --no-enable-prefix-caching \
    --kv-transfer-config "$KV_CONFIG"
