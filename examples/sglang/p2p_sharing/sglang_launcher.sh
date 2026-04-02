#!/bin/bash
# Launch a single SGLang instance with Maru HiCache L3 backend.
#
# Usage:
#   INSTANCE_ID=sglang-inst1 PORT=$SGLANG_INST1_PORT GPU_ID=0 bash sglang_launcher.sh
#
# Environment variables (set via env.sh or caller):
#   INSTANCE_ID   — Unique instance identifier (default: sglang-default)
#   PORT          — SGLang server port (default: $SGLANG_INST1_PORT)
#   GPU_ID        — CUDA device index (default: 0)

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

INSTANCE_ID=${INSTANCE_ID:-sglang-default}
PORT=${PORT:-$SGLANG_INST1_PORT}
GPU_ID=${GPU_ID:-0}
LOG_FILE="${INSTANCE_ID}.log"

# Color prefix for console output (log file stays plain)
case "$INSTANCE_ID" in
    inst1) COLOR=$'\033[1;36m'; TAG="[1   ]" ;;  # bold cyan
    inst2) COLOR=$'\033[1;33m'; TAG="[   2]" ;;  # bold yellow
    *)     COLOR=$'\033[1;32m'; TAG="[${INSTANCE_ID}]" ;;
esac
RESET=$'\033[0m'

export CUDA_VISIBLE_DEVICES="$GPU_ID"

# Build extra config via envsubst
MARU_EXTRA_CONFIG=$(
  MARU_INSTANCE_ID=$INSTANCE_ID \
  MARU_SERVER_PORT=$MARU_SERVER_PORT \
  MARU_POOL_SIZE=$MARU_POOL_SIZE \
  envsubst < "$(dirname "${BASH_SOURCE[0]}")/configs/maru-sglang-p2p.json"
)

echo "[$(date +%T)] Launching SGLang instance: $INSTANCE_ID (port=$PORT, gpu=$GPU_ID)"

sglang serve \
    --model-path "$MODEL" \
    --port "$PORT" \
    --mem-fraction-static "$GPU_MEM_UTIL" \
    --log-level "$SGLANG_LOG_LEVEL" \
    --enable-hierarchical-cache \
    --hicache-ratio 2.0 \
    --hicache-write-policy write_through \
    --hicache-mem-layout page_first_direct \
    --hicache-storage-backend dynamic \
    --hicache-storage-backend-extra-config "$MARU_EXTRA_CONFIG" \
    --hicache-storage-prefetch-policy wait_complete \
    > >(tee "$LOG_FILE" | sed -u "s/^/${COLOR}${TAG}${RESET} /") 2>&1
