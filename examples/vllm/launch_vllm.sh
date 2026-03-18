#!/bin/bash
# Launch a vLLM instance with Maru KV connector (direct, no LMCache)
#
# Usage:
#   ./launch_vllm.sh <inst1|inst2> [model]
#
# Examples:
#   ./launch_vllm.sh inst1                    # Instance 1 with default model
#   ./launch_vllm.sh inst2 Qwen/Qwen2.5-0.5B # Instance 2 with specific model

set -euo pipefail

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.1}"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <inst1|inst2> [model]"
    exit 1
fi

MODEL="${2:-${MODEL:-Qwen/Qwen2.5-0.5B}}"
echo "Using model: ${MODEL}"

if [[ $1 == "inst1" ]]; then
    DEVICE=0
    PORT=$MARU_INST1_PORT
elif [[ $1 == "inst2" ]]; then
    DEVICE=1
    PORT=$MARU_INST2_PORT
else
    echo "Invalid role: $1 (expected inst1 or inst2)"
    exit 1
fi

echo "=== Maru-vLLM Direct Integration ==="
echo "  Instance:    $1"
echo "  GPU Device:  $DEVICE"
echo "  vLLM Port:   $PORT"
echo "  Maru Server: $MARU_SERVER_URL"
echo "  Pool Size:   $MARU_POOL_SIZE"
echo "  Chunk Tokens: $MARU_KV_CHUNK_TOKENS"
echo "====================================="

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

PYTHONHASHSEED=123 \
    CUDA_VISIBLE_DEVICES=$DEVICE \
    vllm serve "$MODEL" \
    --gpu-memory-utilization "$GPU_MEM_UTIL" \
    --port "$PORT" \
    --kv-transfer-config "$KV_CONFIG"
