#!/bin/bash
# Remote CXL KV example on one host: the memory node's region mapper, maru-server and
# both workers run here. The workers still read and write the CXL pool through
# the region mapper's NIC registration and one-sided RDMA, as across nodes.
#
# This script:
#   1. Starts the region mapper (the memory node's service)
#   2. Starts maru-server with that region mapper
#   3. Launches two vLLM engines with storage_backend "remote_cxl"
#   4. Runs the sharing test (inst1 stores, inst2 loads from the pool)
#   5. Cleans up all processes
#
# Requirements on this host: 2 GPUs, an RDMA NIC (RoCE or InfiniBand), the
# Maru resource manager with a CXL/DAX device, and NIXL.
#
# Usage:
#   ./remote_cxl_example.sh [model]
#
# Examples:
#   MARU_DAX_PATH=/dev/dax0.0 MARU_MAPPER_UCX_DEVICE=mlx5_0:1 \
#       MARU_WORKER_UCX_DEVICE=mlx5_0:1 ./remote_cxl_example.sh
#   MARU_POOL_SIZE=4G GPU_MEM_UTIL=0.5 ./remote_cxl_example.sh meta-llama/Llama-3.1-8B-Instruct

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
    # Engines first: they give their regions back through the region mapper
    # and maru-server on the way out. Only the launcher gets TERM; it passes
    # it on to the API server, which shuts the engine down. A TERM that
    # reaches the engine process itself can end it before its regions go
    # back.
    for p in "${INST1_PID:-}" "${INST2_PID:-}" "${SERVER_PID:-}" "${MAPPER_PID:-}"; do
        [[ -z "$p" ]] && continue
        local tree=( $(descendants "$p") )
        if [[ "$p" == "${INST1_PID:-}" || "$p" == "${INST2_PID:-}" ]]; then
            kill -TERM "$p" 2>/dev/null || true
        else
            kill -TERM "${tree[@]}" 2>/dev/null || true
        fi
        for _ in $(seq 1 30); do  # graceful window, then force-kill survivors
            local alive=0
            for q in "${tree[@]}"; do
                if kill -0 "$q" 2>/dev/null; then alive=1; fi
            done
            if [[ $alive -eq 0 ]]; then break; fi
            sleep 1
        done
        for q in "${tree[@]}"; do kill -9 "$q" 2>/dev/null || true; done
    done
    wait 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

wait_for_log() {  # <log> <pattern> <pid> <name>
    for i in $(seq 1 120); do
        if grep -q "$2" "$1" 2>/dev/null; then
            echo "  $4 ready (${i}s)"
            return 0
        fi
        if ! kill -0 "$3" 2>/dev/null || [[ $i -eq 120 ]]; then
            echo "ERROR: $4 did not start. Check $1"
            exit 1
        fi
        sleep 1
    done
}

# A memory node where Maru was installed with NIXL runs the region mapper as a
# system service that starts with the resource manager. Use it then: one
# region mapper serves the node.
MAPPER_SERVICE=""
if systemctl is-active --quiet maru-region-mapper 2>/dev/null; then
    setting() {
        (grep -E "^$1=" /etc/maru/region-mapper.env 2>/dev/null || true) |
            tail -1 | cut -d= -f2-
    }
    host=$(setting MARU_REGION_MAPPER_HOST)
    if [[ -z "$host" ]]; then
        # The service listens on its RDMA NIC's address.
        host=$(python -c "import sys; from maru_region_mapper.cli import rdma_address; print(rdma_address(sys.argv[1]))" \
            "$(setting MARU_REGION_MAPPER_UCX_DEVICE)")
    elif [[ "$host" == "0.0.0.0" ]]; then
        host=127.0.0.1
    fi
    port=$(setting MARU_REGION_MAPPER_PORT)
    MAPPER_SERVICE="$host:${port:-9860}"
    export MARU_REGION_MAPPERS=$MAPPER_SERVICE
    if [[ ! -r "$MARU_REGION_MAPPER_KEY_FILE" ]]; then
        echo "ERROR: no readable copy of the region mapper key at $MARU_REGION_MAPPER_KEY_FILE." >&2
        echo "  sudo install -D -m 0600 -o \"\$USER\" /etc/maru/region-mapper.key $MARU_REGION_MAPPER_KEY_FILE" >&2
        exit 1
    fi
fi

echo "================================================"
echo "  Maru-vLLM remote CXL KV Example (one host)"
echo "================================================"
echo "  Model:          $MODEL"
if [[ -n "$MAPPER_SERVICE" ]]; then
    echo "  Region mapper:  $MAPPER_SERVICE (system service)"
else
    echo "  Region mapper:  $MARU_MEMORY_NODE_HOST:$MARU_MAPPER_PORT"
fi
echo "  maru-server:    $MARU_SERVER_URL (DAX ${MARU_DAX_PATH:-any})"
echo "  Instance 1:     port $MARU_INST1_PORT (GPU $MARU_INST1_GPU)"
echo "  Instance 2:     port $MARU_INST2_PORT (GPU $MARU_INST2_GPU)"
echo "  Region size:    $MARU_POOL_SIZE"
echo "  Chunk Tokens:   $MARU_KV_CHUNK_TOKENS"
echo "================================================"
echo ""

# Step 1: the memory node's region mapper
if [[ -n "$MAPPER_SERVICE" ]]; then
    echo "[Step 1] Using the region mapper service at $MAPPER_SERVICE"
else
    echo "[Step 1] Starting the region mapper on port $MARU_MAPPER_PORT..."
    : > "$LOG_DIR/region_mapper.log"   # the readiness wait must not see an old run
    bash "$SCRIPT_DIR/region_mapper.sh" > "$LOG_DIR/region_mapper.log" 2>&1 &
    MAPPER_PID=$!
    wait_for_log "$LOG_DIR/region_mapper.log" "region mapper for node" "$MAPPER_PID" "region mapper"
fi

# Step 2: maru-server, which hands the region mapper's address to the workers
echo "[Step 2] Starting maru-server on port $MARU_SERVER_PORT..."
: > "$LOG_DIR/maru_server.log"
bash "$SCRIPT_DIR/maru_server.sh" > "$LOG_DIR/maru_server.log" 2>&1 &
SERVER_PID=$!
wait_for_log "$LOG_DIR/maru_server.log" "RPC Server started" "$SERVER_PID" "maru-server"

# Step 3: the two engines
echo "[Step 3] Starting vLLM instance 1 (GPU $MARU_INST1_GPU, port $MARU_INST1_PORT)..."
bash "$SCRIPT_DIR/remote_cxl_vllm_launcher.sh" inst1 "$MODEL" > "$LOG_DIR/inst1.log" 2>&1 &
INST1_PID=$!
echo "[Step 3] Starting vLLM instance 2 (GPU $MARU_INST2_GPU, port $MARU_INST2_PORT)..."
bash "$SCRIPT_DIR/remote_cxl_vllm_launcher.sh" inst2 "$MODEL" > "$LOG_DIR/inst2.log" 2>&1 &
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

# Step 4: the sharing test
echo "[Step 4] Running the remote CXL KV sharing test..."
RC=0
python "$SCRIPT_DIR/run_benchmark.py" \
    --model "$MODEL" \
    --url1 "$MARU_INST1_URL" \
    --url2 "$MARU_INST2_URL" || RC=$?

# The cached-token count shows the prefix was found in the pool. A load that
# fails afterwards (RDMA error, timeout, missing key) is logged by the
# connector and recomputed.
LOAD_FAILED='Maru (deferred )?load failed|Maru batch_retrieve failed'
if grep -qE "$LOAD_FAILED" "$LOG_DIR/inst2.log"; then
    echo "ERROR: instance 2 found the prefix but failed to load it:"
    grep -E "$LOAD_FAILED" "$LOG_DIR/inst2.log" | tail -3
    RC=1
elif [[ $RC -eq 0 ]]; then
    echo "Instance 2 loaded the prefix from the pool (no load failure in inst2.log)."
fi

echo ""
echo "Logs:"
echo "  region mapper: $LOG_DIR/region_mapper.log"
echo "  maru-server:   $LOG_DIR/maru_server.log"
echo "  Instance 1:    $LOG_DIR/inst1.log"
echo "  Instance 2:    $LOG_DIR/inst2.log"
exit $RC
