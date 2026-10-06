#!/bin/bash
# Remote KV example on one host: the pool node and both workers run here and
# talk through NIXL as they would across nodes. (On one host UCX may pick a
# shared-memory transport instead of the NIC.)
#
# This script:
#   1. Starts maru-server with its remote endpoint (the pool node)
#   2. Launches two vLLM engines with storage_backend "remote"
#   3. Runs the sharing test (inst1 stores, inst2 loads from the pool)
#   4. Cleans up all processes
#
# Requirements on this host: 2 GPUs, an RDMA NIC, the Maru resource manager
# with a CXL/DAX device, and NIXL (pip install 'maru[remote]').
#
# Usage:
#   ./remote_example.sh [model]
#
# Examples:
#   ./remote_example.sh                          # Default: Qwen/Qwen2.5-0.5B
#   MARU_REMOTE_PAGE_BYTES=32M GPU_MEM_UTIL=0.5 \
#       ./remote_example.sh meta-llama/Llama-3.1-8B-Instruct

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Everything runs on this host, so keep the remote endpoint on loopback.
export MARU_REMOTE_BIND_HOST=${MARU_REMOTE_BIND_HOST:-127.0.0.1}
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
    if [[ -n "${POOL_PID:-}" ]]; then pids+=( $(descendants "$POOL_PID") ); fi
    if [[ ${#pids[@]} -gt 0 ]]; then
        kill -TERM "${pids[@]}" 2>/dev/null || true
        for _ in $(seq 1 15); do  # graceful window, then force-kill survivors
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
echo "  Maru-vLLM Remote KV Example (one host)"
echo "================================================"
echo "  Model:        $MODEL"
echo "  Pool:         $MARU_REMOTE_URL (DAX ${MARU_DAX_PATH:-any})"
echo "  Instance 1:   port $MARU_INST1_PORT (GPU $MARU_INST1_GPU)"
echo "  Instance 2:   port $MARU_INST2_PORT (GPU $MARU_INST2_GPU)"
echo "  Pool size:    $MARU_REMOTE_POOL_SIZE, page $MARU_REMOTE_PAGE_BYTES"
echo "  Chunk Tokens: $MARU_KV_CHUNK_TOKENS"
echo "================================================"
echo ""

# Step 1: Start the pool node's maru-server with its remote endpoint
echo "[Step 1] Starting maru-server with remote access on port $MARU_REMOTE_PORT..."
: > "$LOG_DIR/maru_server.log"   # the readiness wait below must not see an old run
bash "$SCRIPT_DIR/remote_pool_server.sh" > "$LOG_DIR/maru_server.log" 2>&1 &
POOL_PID=$!
# The server maps and registers the pool, opens the remote endpoint, then
# starts its local RPC server.
for i in $(seq 1 120); do
    if grep -q "RPC Server started" "$LOG_DIR/maru_server.log" 2>/dev/null; then
        echo "  maru-server ready (${i}s)"
        break
    fi
    if ! kill -0 "$POOL_PID" 2>/dev/null || [[ $i -eq 120 ]]; then
        echo "ERROR: maru-server did not start. Check $LOG_DIR/maru_server.log"
        exit 1
    fi
    sleep 1
done

# Step 2: Launch the two engines
echo "[Step 2] Starting vLLM instance 1 (GPU $MARU_INST1_GPU, port $MARU_INST1_PORT)..."
bash "$SCRIPT_DIR/remote_vllm_launcher.sh" inst1 "$MODEL" > "$LOG_DIR/inst1.log" 2>&1 &
INST1_PID=$!
echo "[Step 2] Starting vLLM instance 2 (GPU $MARU_INST2_GPU, port $MARU_INST2_PORT)..."
bash "$SCRIPT_DIR/remote_vllm_launcher.sh" inst2 "$MODEL" > "$LOG_DIR/inst2.log" 2>&1 &
INST2_PID=$!

echo ""
echo "Waiting for vLLM instances to be ready..."
for url in "$MARU_INST1_URL" "$MARU_INST2_URL"; do
    for i in $(seq 1 300); do
        if curl -s "$url/health" > /dev/null 2>&1; then
            echo "  $url ready (${i}s)"
            break
        fi
        if [[ $i -eq 300 ]]; then
            echo "ERROR: $url not ready after 300s. Check inst1.log / inst2.log."
            exit 1
        fi
        sleep 1
    done
done
echo ""

# Step 3: Run the sharing test
echo "[Step 3] Running the remote KV sharing test..."
RC=0
python "$SCRIPT_DIR/run_benchmark.py" \
    --model "$MODEL" \
    --url1 "$MARU_INST1_URL" \
    --url2 "$MARU_INST2_URL" || RC=$?

# The cached-token count shows the prefix was found in the pool. Every load
# that fails afterwards (RDMA error, timeout, missing key) is logged by the
# connector as "Maru load failed for req ... recomputing" and recomputed.
LOAD_FAILED='Maru (deferred )?load failed|Maru batch_retrieve failed'
if grep -qE "$LOAD_FAILED" "$LOG_DIR/inst2.log"; then
    echo "ERROR: instance 2 found the prefix but failed to load it:"
    grep -E "$LOAD_FAILED" "$LOG_DIR/inst2.log" | tail -3
    RC=1
elif [[ $RC -eq 0 ]]; then
    echo "Instance 2 loaded the prefix from the remote pool (no load failure in inst2.log)."
fi

echo ""
echo "Logs:"
echo "  maru-server: $LOG_DIR/maru_server.log"
echo "  Instance 1:  $LOG_DIR/inst1.log"
echo "  Instance 2:  $LOG_DIR/inst2.log"
exit $RC
