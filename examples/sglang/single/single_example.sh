#!/bin/bash
# Single SGLang instance with Maru HiCache L3 backend.
#
# Usage:
#   bash single_example.sh                     # Default model
#   bash single_example.sh --model Qwen/Qwen2.5-7B

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PIDS=()

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
    echo "Stopping everything..."
    trap - INT TERM USR1 EXIT

    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing process tree of $pid"
            kill_tree "$pid" TERM
        fi
    done

    sleep 2

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
            if [ -f "$LOG_SGLANG" ]; then
                echo "  [last log] $(tail -1 "$LOG_SGLANG" 2>/dev/null)"
            fi
            last_report=$now
        fi
        if (( now - start_time >= timeout_seconds )); then
            echo "[$(date +%T)] Timeout waiting for server on port $port (${timeout_seconds}s)"
            if [ -f "$LOG_SGLANG" ]; then
                echo "--- sglang log (last 20 lines) ---"
                tail -20 "$LOG_SGLANG" 2>/dev/null
            fi
            return 1
        fi
        sleep 1
    done
}


main() {
    echo "Using Maru storage backend (single instance)..."

    ensure_python_library_installed sglang
    ensure_python_library_installed maru_sglang

    trap cleanup INT TERM USR1 EXIT

    # Launch MaruServer
    if timeout 1 bash -c "echo >/dev/tcp/localhost/$MARU_SERVER_PORT" 2>/dev/null; then
        echo "[$(date +%T)] MaruServer already running on port $MARU_SERVER_PORT, skipping launch..."
    else
        echo "[$(date +%T)] Launching MaruServer on port $MARU_SERVER_PORT..."
        PYTHONUNBUFFERED=1 python3 -m maru_server --port "$MARU_SERVER_PORT" --log-level "${_LOG_LEVEL:-INFO}" \
            > >(tee "$SCRIPT_DIR/maru_server.log") 2>&1 &
        maru_server_pid=$!
        PIDS+=($maru_server_pid)
        echo "[$(date +%T)] MaruServer PID: $maru_server_pid (log: maru_server.log)"
        sleep 2
        if ! kill -0 $maru_server_pid 2>/dev/null; then
            echo "[$(date +%T)] ERROR: MaruServer process died! Log:"
            cat "$SCRIPT_DIR/maru_server.log" 2>/dev/null || true
            return 1
        fi
        wait_for_server "$MARU_SERVER_PORT"
        echo "[$(date +%T)] MaruServer ready."
    fi

    # Log file name
    LOG_SGLANG="${LOG_SGLANG:-sglang_server.log}"

    echo "[$(date +%T)] Launching SGLang instance (MODEL=$MODEL, GPU_MEM_UTIL=$GPU_MEM_UTIL)..."
    echo "Please check $LOG_SGLANG for logs."

    # Build Maru extra config
    MARU_EXTRA_CONFIG="{\"backend_name\":\"maru\",\"module_path\":\"maru_sglang.maru_storage\",\"class_name\":\"MaruStorage\",\"maru_server_url\":\"tcp://localhost:${MARU_SERVER_PORT}\",\"maru_pool_size\":\"${MARU_POOL_SIZE:-4G}\"}"

    # Launch SGLang with Maru L3 backend
    sglang serve \
        --model-path "$MODEL" \
        --port "$SGLANG_PORT" \
        --mem-fraction-static "$GPU_MEM_UTIL" \
        --log-level "$SGLANG_LOG_LEVEL" \
        --enable-hierarchical-cache \
        --hicache-ratio 2.0 \
        --hicache-write-policy write_through \
        --hicache-mem-layout page_first_direct \
        --hicache-storage-backend dynamic \
        --hicache-storage-backend-extra-config "$MARU_EXTRA_CONFIG" \
        > >(tee "$LOG_SGLANG") 2>&1 &
    PIDS+=($!)

    wait_for_server "$SGLANG_PORT"

    echo "==================================================="
    echo "Server is up. You can send requests now..."
    echo "  Port: $SGLANG_PORT"
    echo "Press Ctrl-C to terminate."
    echo "==================================================="

    while true; do
        sleep 1
    done
}

# --- Help ---
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Launch a single SGLang instance with Maru HiCache L3 backend."
    echo "Send the same query twice to verify KV cache hit."
    echo ""
    echo "Options:"
    echo "  --model MODEL          HuggingFace model name (default: $MODEL)"
    echo "  --log-level LEVEL      Log level: DEBUG, INFO, WARNING, ERROR (default: INFO)"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Environment variables (from env.sh):"
    echo "  SGLANG_PORT            SGLang port (default: UID-based)"
    echo "  MARU_SERVER_PORT       MaruServer port (default: UID-based)"
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

if [[ -n "$_LOG_LEVEL" ]]; then
    export SGLANG_LOG_LEVEL="$_LOG_LEVEL"
    export MARU_LOG_LEVEL="$_LOG_LEVEL"
fi
if [[ -n "$_MODEL" ]]; then
    export MODEL="$_MODEL"
fi

main
