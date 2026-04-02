#!/bin/bash
# P2P KV sharing example — manages MaruServer + two SGLang instances.
#
# Usage:
#   bash p2p_example.sh                    # Default model
#   bash p2p_example.sh --model Qwen/Qwen2.5-7B
#   bash p2p_example.sh --log-level WARNING

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PIDS=()

check_num_gpus() {
    num_gpus=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    if [ "$num_gpus" -lt 2 ]; then
        echo "You need at least 2 GPUs to run KV cache sharing."
        exit 1
    else
        echo "Found $num_gpus GPUs."
    fi
}

ensure_python_library_installed() {
    echo "Checking if $1 is installed..."
    python3 -c "import $1" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "$1 is not installed. Please install it via pip install $1."
        exit 1
    else
        echo "$1 is installed."
    fi
}

kill_tree() {
    local pid=$1 sig=${2:-TERM}
    for child in $(pgrep -P "$pid" 2>/dev/null); do
        kill_tree "$child" "$sig"
    done
    kill -"$sig" "$pid" 2>/dev/null
}

cleanup() {
    echo "Stopping everything…"
    trap - INT TERM USR1 EXIT   # prevent re-entrancy

    # Graceful: recursively kill all tracked process trees
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing process tree of $pid"
            kill_tree "$pid" TERM
        fi
    done

    sleep 2

    # Force kill any survivors
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force killing process tree of $pid"
            kill_tree "$pid" 9
        fi
    done

    echo "All processes stopped."
    exit 0
}

wait_for_server() {
    local port=$1
    local timeout_seconds=1200
    local start_time=$(date +%s)
    local last_report=$start_time

    echo "[$(date +%T)] Waiting for server on port $port (timeout: ${timeout_seconds}s)..."

    while true; do
        if [ "$port" = "$MARU_SERVER_PORT" ]; then
            if timeout 1 bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null; then
                echo "[$(date +%T)] Server on port $port is ready"
                return 0
            fi
        else
            if curl -s "localhost:${port}/health" > /dev/null 2>&1; then
                echo "[$(date +%T)] Server on port $port is ready"
                return 0
            fi
        fi

        local now=$(date +%s)
        if (( now - last_report >= 30 )); then
            local elapsed=$(( now - start_time ))
            echo "[$(date +%T)] Still waiting for port $port... (${elapsed}s elapsed)"
            last_report=$now
        fi
        if (( now - start_time >= timeout_seconds )); then
            echo "[$(date +%T)] Timeout waiting for server on port $port (${timeout_seconds}s)"
            return 1
        fi
        sleep 1
    done
}


main() {
    echo "=== P2P KV Sharing Example (SGLang + Maru) ==="
    echo "Model: $MODEL"
    echo "MaruServer port: $MARU_SERVER_PORT"
    echo "Instance1: port=$SGLANG_INST1_PORT, gpu=$INST1_GPU"
    echo "Instance2: port=$SGLANG_INST2_PORT, gpu=$INST2_GPU"
    echo ""

    check_num_gpus
    ensure_python_library_installed sglang
    ensure_python_library_installed maru_sglang

    trap cleanup INT TERM USR1 EXIT

    # 1. Maru Meta Server
    if timeout 1 bash -c "echo >/dev/tcp/localhost/$MARU_SERVER_PORT" 2>/dev/null; then
        echo "[$(date +%T)] MaruServer already running on port $MARU_SERVER_PORT, skipping launch..."
    else
        echo "[$(date +%T)] Launching MaruServer on port $MARU_SERVER_PORT..."
        PYTHONUNBUFFERED=1 python3 -m maru_server --port "$MARU_SERVER_PORT" \
            > >(tee "$SCRIPT_DIR/maru_server.log") 2>&1 &
        PIDS+=($!)
        sleep 2
        if ! kill -0 "${PIDS[-1]}" 2>/dev/null; then
            echo "[$(date +%T)] ERROR: MaruServer died! Check maru_server.log"
            return 1
        fi
        wait_for_server "$MARU_SERVER_PORT"
        echo "[$(date +%T)] MaruServer ready."
    fi

    # Log file names
    LOG_INST1="${LOG_INST1:-inst1.log}"
    LOG_INST2="${LOG_INST2:-inst2.log}"

    echo "[$(date +%T)] Launching SGLang instances (MODEL=$MODEL, GPU_MEM_UTIL=$GPU_MEM_UTIL)..."
    echo "Please check $LOG_INST1 and $LOG_INST2 for logs."

    # 2. SGLang Instance 1 (start first so inst2 can pre-map its region)
    INSTANCE_ID=inst1 PORT=$SGLANG_INST1_PORT GPU_ID=$INST1_GPU \
        bash "$SCRIPT_DIR/sglang_launcher.sh" &
    PIDS+=($!)

    wait_for_server "$SGLANG_INST1_PORT"

    # 3. SGLang Instance 2 (starts after inst1 is ready — sees inst1's shared region)
    INSTANCE_ID=inst2 PORT=$SGLANG_INST2_PORT GPU_ID=$INST2_GPU \
        bash "$SCRIPT_DIR/sglang_launcher.sh" &
    PIDS+=($!)

    wait_for_server "$SGLANG_INST2_PORT"

    echo ""
    echo "==================================================="
    echo "All servers are up. You can send request now..."
    echo "Press Ctrl-C to terminate all instances."
    echo ""
    echo "Quick test:"
    echo "  bash run_simple_query.sh"
    echo ""
    echo "Benchmark:"
    echo "  bash run_benchmark.sh"
    echo "==================================================="

    while true; do
        sleep 1
    done
}

# --- Help ---
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Launch two SGLang instances with Maru shared KV cache sharing."
    echo "Servers stay running until Ctrl-C."
    echo ""
    echo "Options:"
    echo "  --model MODEL          HuggingFace model name (default: $MODEL)"
    echo "  --log-level LEVEL      Log level: DEBUG, INFO, WARNING, ERROR (default: INFO)"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                     # Default model"
    echo "  $0 --model Qwen/Qwen2.5-7B"
    echo "  $0 --log-level WARNING"
    echo ""
    echo "Environment variables (from env.sh):"
    echo "  MARU_SERVER_PORT       MaruServer port (default: UID-based)"
    echo "  SGLANG_INST1_PORT      Instance 1 port (default: UID-based)"
    echo "  SGLANG_INST2_PORT      Instance 2 port (default: UID-based)"
    echo "  GPU_MEM_UTIL           GPU memory utilization (default: 0.9)"
    exit 0
}

# --- Argument parsing ---
_LOG_LEVEL=""
_MODEL=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)   usage ;;
        --log-level) _LOG_LEVEL="$2"; shift 2 ;;
        --model)     _MODEL="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Apply overrides
if [[ -n "$_LOG_LEVEL" ]]; then
    # export SGLANG_LOG_LEVEL="$_LOG_LEVEL"
    export MARU_LOG_LEVEL="$_LOG_LEVEL"
fi
if [[ -n "$_MODEL" ]]; then
    export MODEL="$_MODEL"
fi

main
