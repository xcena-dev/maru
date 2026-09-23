#!/bin/bash
# Full single-node Maru x Dynamo example: cross-instance KV sharing behind
# a Dynamo frontend router.
#
# This script:
#   1. Starts maru-server
#   2. Starts the Dynamo frontend (BEFORE any worker — see note below)
#   3. Launches two dynamo.vllm workers with MaruKVConnector
#   4. Waits until the frontend actually serves completions
#   5. Runs the cross-instance sharing test (store on one worker,
#      retrieve on the other, both through the frontend)
#   6. Cleans up all processes
#
# ORDERING RULE: with file discovery, a worker's `generate` registration is
# lease-based and expires within seconds unless a watching frontend renews
# it. A frontend started after the workers then answers 503 for every
# request, forever. The frontend must be up before the first worker starts.
#
# Usage:
#   ./single_node_example.sh [model]
#
# Example:
#   ./single_node_example.sh                  # Default: Qwen/Qwen2.5-0.5B
#   W0_GPU=0 W1_GPU=0 ./single_node_example.sh  # both workers on one GPU

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

MODEL="${1:-${MODEL:-Qwen/Qwen2.5-0.5B}}"
LOG_DIR="$SCRIPT_DIR"

cleanup() {
    echo ""
    echo "Cleaning up..."
    # LAST_WORKER_PID covers a worker whose start_worker attempt failed
    # before its W0_PID/W1_PID assignment (double-kill is harmless).
    [[ -n "${LAST_WORKER_PID:-}" ]] && kill "$LAST_WORKER_PID" 2>/dev/null
    [[ -n "${W0_PID:-}" ]] && kill "$W0_PID" 2>/dev/null && echo "  Stopped worker w0 (PID $W0_PID)"
    [[ -n "${W1_PID:-}" ]] && kill "$W1_PID" 2>/dev/null && echo "  Stopped worker w1 (PID $W1_PID)"
    [[ -n "${FE_PID:-}" ]] && kill "$FE_PID" 2>/dev/null && echo "  Stopped frontend (PID $FE_PID)"
    [[ -n "${MARU_PID:-}" ]] && kill "$MARU_PID" 2>/dev/null && echo "  Stopped maru-server (PID $MARU_PID)"
    wait 2>/dev/null
    echo "Done."
}
trap cleanup EXIT

echo "=================================================="
echo "  Maru x Dynamo Single-Node KV Sharing Example"
echo "=================================================="
echo "  Model:        $MODEL"
echo "  Maru Server:  $MARU_SERVER_URL"
echo "  Frontend:     http://localhost:$DYN_HTTP_PORT (router-mode direct)"
echo "  Worker w0:    GPU $W0_GPU, health :$DYN_W0_SYSTEM_PORT"
echo "  Worker w1:    GPU $W1_GPU, health :$DYN_W1_SYSTEM_PORT"
echo "  Discovery:    $DISCOVERY_BACKEND$( [[ $DISCOVERY_BACKEND == file ]] && echo " ($DYN_FILE_KV)" || echo " ($ETCD_ENDPOINTS)" )"
echo "=================================================="
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

# Step 2: Start the frontend FIRST (see ORDERING RULE above). Wipe stale
# discovery state so dead registrations from previous runs can't linger.
echo "[Step 2] Starting Dynamo frontend on port $DYN_HTTP_PORT..."
if [[ "$DISCOVERY_BACKEND" == "file" ]]; then
    rm -rf "$DYN_FILE_KV"
fi
bash "$SCRIPT_DIR/dynamo_launcher.sh" frontend > "$LOG_DIR/frontend.log" 2>&1 &
FE_PID=$!
for i in $(seq 1 30); do
    if curl -s "http://localhost:$DYN_HTTP_PORT/health" > /dev/null 2>&1; then
        echo "  Frontend ready (${i}s)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "ERROR: Frontend not ready after 30s. Check $LOG_DIR/frontend.log"
        exit 1
    fi
    sleep 1
done

# Step 3: Launch the two workers SEQUENTIALLY. Concurrent starts race in
# vLLM's memory profiling when both workers share a GPU (the second sees
# the first mid-allocation and computes a negative KV budget); sequential
# starts also make the /health registration count an unambiguous ready
# signal per worker.
#
# File discovery on some hosts occasionally drops or misses a worker's
# `generate` registration (the worker's log shows "Registered endpoint
# ...generate" but it never appears in the frontend registry, or shows up
# and is later Expired). Restarting just the worker recovers — the
# frontend and maru-server stay up — so each worker gets one automatic
# retry before the script gives up.
generate_count() {
    curl -s "http://localhost:$DYN_HTTP_PORT/health" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(sum(1 for x in d.get('instances', []) if x.get('endpoint') == 'generate'))
except Exception:
    print(0)
" 2>/dev/null || echo 0
}

# start_worker <w0|w1> <expected_generate_count> — sets LAST_WORKER_PID
start_worker() {
    local worker="$1" want="$2"
    for attempt in 1 2; do
        echo "[Step 3] Starting worker $worker (attempt $attempt)..."
        bash "$SCRIPT_DIR/dynamo_launcher.sh" worker "$worker" "$MODEL" \
            > "$LOG_DIR/$worker.log" 2>&1 &
        LAST_WORKER_PID=$!
        for i in $(seq 1 120); do
            if [[ "$(generate_count)" == "$want" ]]; then
                echo "  worker $worker registered (${i}s)"
                return 0
            fi
            if ! kill -0 "$LAST_WORKER_PID" 2>/dev/null; then
                echo "  worker $worker exited during startup — check $LOG_DIR/$worker.log"
                break
            fi
            sleep 1
        done
        if [[ $attempt -eq 1 ]]; then
            echo "  worker $worker never appeared in the frontend registry —"
            echo "  restarting it (known file-discovery flake; see README)"
            kill "$LAST_WORKER_PID" 2>/dev/null || true
            wait "$LAST_WORKER_PID" 2>/dev/null || true
            sleep 3
        fi
    done
    echo "ERROR: worker $worker not registered after 2 attempts."
    echo "  Frontend registry at timeout:"
    curl -s "http://localhost:$DYN_HTTP_PORT/health" | python3 -m json.tool 2>/dev/null | head -40 || true
    exit 1
}

start_worker w0 1
W0_PID=$LAST_WORKER_PID
start_worker w1 2
W1_PID=$LAST_WORKER_PID

# Step 4b: Wait until the frontend actually serves a completion. It keeps
# returning 503 briefly after registration until its health canary against
# the workers passes — probe with a real 1-token request, not /health.
# In direct router mode every request needs an explicit target (an
# untargeted request is a 400), so route the probe to the first worker.
PROBE_ID=$(curl -s "http://localhost:$DYN_HTTP_PORT/health" | python3 -c "
import json, sys
d = json.load(sys.stdin)
ids = sorted(x['instance_id'] for x in d.get('instances', []) if x.get('endpoint') == 'generate')
print(ids[0])
")
echo "Waiting for the frontend to serve completions (probe -> worker $PROBE_ID)..."
for i in $(seq 1 120); do
    CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
        -X POST "http://localhost:$DYN_HTTP_PORT/v1/completions" \
        -H "Content-Type: application/json" \
        -d "{\"model\": \"$MODEL\", \"prompt\": \"hi\", \"max_tokens\": 1, \"temperature\": 0, \"nvext\": {\"backend_instance_id\": $PROBE_ID}}")
    if [[ "$CODE" == "200" ]]; then
        echo "  Frontend serving completions (${i} probes)"
        break
    fi
    if [[ $i -eq 120 ]]; then
        echo "ERROR: frontend never served a completion (last HTTP $CODE)."
        echo "  If the frontend logs show 'storage::kv::file: Expired' for a"
        echo "  worker's generate registration, restart that worker — the"
        echo "  frontend and maru-server can stay up."
        exit 1
    fi
    sleep 2
done

echo ""

# Step 5: Run the cross-instance sharing test
echo "[Step 5] Running cross-instance KV sharing test..."
echo ""
bash "$SCRIPT_DIR/run_simple_query.sh"

echo ""
echo "Logs:"
echo "  maru-server: $LOG_DIR/maru_server.log"
echo "  frontend:    $LOG_DIR/frontend.log"
echo "  worker w0:   $LOG_DIR/w0.log"
echo "  worker w1:   $LOG_DIR/w1.log"
