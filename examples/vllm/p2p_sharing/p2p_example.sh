#!/bin/bash
# Full P2P KV cache sharing example with Maru-vLLM direct integration
#
# This script:
#   1. Starts maru-server
#   2. Launches two vLLM instances with MaruKVConnector
#   3. Runs the P2P test (store on inst1, retrieve on inst2)
#   4. Cleans up all processes
#
# Usage:
#   ./p2p_example.sh [model]
#
# Example:
#   ./p2p_example.sh                     # Default: Qwen/Qwen2.5-0.5B
#   ./p2p_example.sh meta-llama/Llama-3-8B

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

MODEL="${1:-${MODEL:-Qwen/Qwen2.5-0.5B}}"
LOG_DIR="$SCRIPT_DIR"

# vLLM spawns a subprocess tree (API server + EngineCore). Killing only the
# launcher PID leaks the children: they reparent to init and keep holding the
# GPU. So collect the whole tree first, then TERM, then SIGKILL any survivor.
descendants() {
    local pid=$1 child
    echo "$pid"
    for child in $(pgrep -P "$pid" 2>/dev/null); do descendants "$child"; done
}

cleanup() {
    echo ""
    echo "Cleaning up..."
    local pids=() p
    if [[ -n "${INST1_PID:-}" ]]; then pids+=( $(descendants "$INST1_PID") ); fi
    if [[ -n "${INST2_PID:-}" ]]; then pids+=( $(descendants "$INST2_PID") ); fi
    if [[ -n "${MARU_PID:-}" ]]; then pids+=( $(descendants "$MARU_PID") ); fi
    if [[ ${#pids[@]} -gt 0 ]]; then
        kill -TERM "${pids[@]}" 2>/dev/null || true
        for _ in $(seq 1 8); do  # graceful window, then force-kill survivors
            local alive=0
            for p in "${pids[@]}"; do
                if kill -0 "$p" 2>/dev/null; then alive=1; fi
            done
            if [[ $alive -eq 0 ]]; then break; fi
            sleep 1
        done
        for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
    fi
    wait 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

echo "================================================"
echo "  Maru-vLLM Direct P2P KV Cache Sharing Example"
echo "================================================"
echo "  Model:       $MODEL"
echo "  Maru Server: $MARU_SERVER_URL"
echo "  Instance 1:  port $MARU_INST1_PORT (GPU 0)"
echo "  Instance 2:  port $MARU_INST2_PORT (GPU 1)"
echo "  Pool Size:   $MARU_POOL_SIZE"
echo "  Chunk Tokens: $MARU_KV_CHUNK_TOKENS"
echo "================================================"
echo ""

# Step 1: Start maru-server
echo "[Step 1] Starting maru-server on port $MARU_SERVER_PORT..."
maru-server --port "$MARU_SERVER_PORT" > "$LOG_DIR/maru_server.log" 2>&1 &
MARU_PID=$!
sleep 2

if ! kill -0 "$MARU_PID" 2>/dev/null; then
    echo "ERROR: maru-server failed to start. Check $LOG_DIR/maru_server.log"
    exit 1
fi
echo "  maru-server started (PID $MARU_PID)"

# Step 2: Launch vLLM instance 1
echo "[Step 2] Starting vLLM instance 1 (GPU 0, port $MARU_INST1_PORT)..."
bash "$SCRIPT_DIR/p2p_vllm_launcher.sh" inst1 "$MODEL" > "$LOG_DIR/inst1.log" 2>&1 &
INST1_PID=$!

# Step 3: Launch vLLM instance 2
echo "[Step 3] Starting vLLM instance 2 (GPU 1, port $MARU_INST2_PORT)..."
bash "$SCRIPT_DIR/p2p_vllm_launcher.sh" inst2 "$MODEL" > "$LOG_DIR/inst2.log" 2>&1 &
INST2_PID=$!

# Wait for instances to be ready
echo ""
echo "Waiting for vLLM instances to be ready..."
for port in "$MARU_INST1_PORT" "$MARU_INST2_PORT"; do
    for i in $(seq 1 120); do
        if curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
            echo "  Port $port ready (${i}s)"
            break
        fi
        if [[ $i -eq 120 ]]; then
            echo "ERROR: Port $port not ready after 120s. Check logs."
            exit 1
        fi
        sleep 1
    done
done

echo ""

# Step 4: Run test
echo "[Step 4] Running P2P KV cache sharing test..."
echo ""
python "$SCRIPT_DIR/run_benchmark.py" \
    --model "$MODEL" \
    --port1 "$MARU_INST1_PORT" \
    --port2 "$MARU_INST2_PORT" \
    --wait-time 3.0

echo ""
echo "Logs:"
echo "  maru-server: $LOG_DIR/maru_server.log"
echo "  Instance 1:  $LOG_DIR/inst1.log"
echo "  Instance 2:  $LOG_DIR/inst2.log"
