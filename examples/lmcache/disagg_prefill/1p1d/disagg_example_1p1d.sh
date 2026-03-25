#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

echo "Warning: LMCache disaggregated prefill support for vLLM v1 is experimental and subject to change."

# Load common environment variables
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

PIDS=()

# Switch to the directory of the current script
cd "$(dirname "${BASH_SOURCE[0]}")"

check_hf_token() {
    if [ -z "$HF_TOKEN" ]; then
        echo "HF_TOKEN is not set. Please set it to your Hugging Face token."
        exit 1
    fi
    if [[ "$HF_TOKEN" != hf_* ]]; then
        echo "HF_TOKEN is not a valid Hugging Face token. Please set it to your Hugging Face token."
        exit 1
    fi
    echo "HF_TOKEN is set and valid."
}

check_num_gpus() {
    num_gpus=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    if [ "$num_gpus" -lt 2 ]; then
        echo "You need at least 2 GPUs to run disaggregated prefill."
        exit 1
    else
        echo "Found $num_gpus GPUs."
    fi
}

ensure_python_library_installed() {
    echo "Checking if $1 is installed..."
    python -c "import $1" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "$1 is not installed. Please install it via pip install $1."
        exit 1
    else
        echo "$1 is installed."
    fi
}


kill_tree() {
    # Recursively kill a process and all its descendants
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

    # Wait a moment for graceful shutdown
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

  echo "Waiting for server on port $port..."

  while true; do
    # MaruServer (ZeroMQ)
    if [ "$port" = "$MARU_SERVER_PORT" ]; then
      if nc -z localhost "$port" 2>/dev/null; then
        echo "MaruServer is ready"
        return 0
      fi
    # vLLM / Proxy server
    else
      if curl -s "localhost:${port}/health" > /dev/null 2>&1 || \
         curl -s "localhost:${port}/docs" > /dev/null 2>&1; then
        echo "Server on port $port is ready"
        return 0
      fi
    fi

    local now=$(date +%s)
    if (( now - start_time >= timeout_seconds )); then
      echo "Timeout waiting for server on port $port"
      return 1
    fi

    sleep 1
  done
}


main() {
    echo "Using Maru shared storage backend..."

    # check_hf_token
    check_num_gpus
    ensure_python_library_installed lmcache
    ensure_python_library_installed pandas
    ensure_python_library_installed datasets
    ensure_python_library_installed vllm

    trap cleanup INT TERM USR1 EXIT

    # Launch MaruServer
    if nc -z localhost $MARU_SERVER_PORT 2>/dev/null; then
        echo "MaruServer already running on port $MARU_SERVER_PORT, skipping launch..."
    else
        echo "Launching MaruServer..."
        PYTHONUNBUFFERED=1 python -m maru_server --port $MARU_SERVER_PORT --log-level "${_LOG_LEVEL:-ERROR}" \
            > >(tee "${LOG_MARU_SERVER:-maru_server.log}") 2>&1 &
        maru_server_pid=$!
        PIDS+=($maru_server_pid)

        wait_for_server $MARU_SERVER_PORT
    fi

    # Log file names (injectable via env vars)
    LOG_PREFILLER="${LOG_PREFILLER:-prefiller.log}"
    LOG_DECODER="${LOG_DECODER:-decoder.log}"
    LOG_PROXY="${LOG_PROXY:-proxy.log}"

    echo "Launching prefiller, decoder and proxy..."
    echo "Please check $LOG_PREFILLER, $LOG_DECODER and $LOG_PROXY for logs."

    # Proxy skips wait_decode_kv_ready (pull-based shared storage mode)
    echo "Proxy will skip wait_decode_kv_ready (shared storage mode)"

    # Launch the proxy first
    python3 ../disagg_proxy_server.py \
        --host localhost \
        --port $LMCACHE_PROXY_EXTERNAL_PORT \
        --prefiller-host localhost \
        --prefiller-port $LMCACHE_PREFILLER_PORT \
        --num-prefillers 1 \
        --decoder-host localhost \
        --decoder-port $LMCACHE_DECODER_PORT  \
        --decoder-init-port $LMCACHE_DECODER_INIT_PORT \
        --decoder-alloc-port $LMCACHE_DECODER_ALLOC_PORT \
        --proxy-host localhost \
        --proxy-port $LMCACHE_PROXY_PORT \
        --num-decoders 1 \
        --skip-kv-wait \
        > >(tee "$LOG_PROXY") 2>&1 &
    proxy_pid=$!
    PIDS+=($proxy_pid)


    # Launch the decoder
    bash disagg_vllm_launcher.sh decoder ${_MODEL:+"$_MODEL"} \
        > >(tee "$LOG_DECODER") 2>&1 &
    decoder_pid=$!
    PIDS+=($decoder_pid)


    # Launch the prefiller next
    bash disagg_vllm_launcher.sh prefiller ${_MODEL:+"$_MODEL"} \
        > >(tee "$LOG_PREFILLER") 2>&1 &
    prefiller_pid=$!
    PIDS+=($prefiller_pid)

    wait_for_server $LMCACHE_DECODER_PORT
    wait_for_server $LMCACHE_PREFILLER_PORT
    wait_for_server $LMCACHE_PROXY_EXTERNAL_PORT

    echo "==================================================="
    echo "All servers are up. You can send request now..."
    echo "Press Ctrl-C to terminate all instances."

    # Keep the script running until interrupted
    echo "Script is running. Waiting for termination signal..."
    echo "==================================================="

    while true; do
        sleep 1
    done
}

# --- Help ---
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Launch prefiller, decoder, and proxy for disaggregated prefill with Maru."
    echo "Servers stay running until Ctrl-C."
    echo ""
    echo "Options:"
    echo "  --model MODEL          HuggingFace model name (default: Qwen/Qwen2.5-0.5B)"
    echo "  --log-level LEVEL      Log level: DEBUG, INFO, WARNING, ERROR"
    echo "                         (default: VLLM=DEBUG, LMCACHE=ERROR from env.sh)"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                     # Default model"
    echo "  $0 --model Qwen/Qwen2.5-7B"
    echo "  $0 --log-level WARNING"
    echo ""
    echo "Environment variables (from env.sh):"
    echo "  LMCACHE_PORT_BASE          Port base (default: 9000 + UID)"
    echo "  LMCACHE_PREFILLER_PORT     Prefiller port (default: PORT_BASE + 100)"
    echo "  LMCACHE_DECODER_PORT       Decoder port (default: PORT_BASE + 200)"
    echo "  LMCACHE_PROXY_EXTERNAL_PORT  Proxy external port (default: PORT_BASE + 2100)"
    echo "  GPU_MEM_UTIL               GPU memory utilization (default: 0.9)"
    echo "  LOG_PREFILLER              Prefiller log file (default: prefiller.log)"
    echo "  LOG_DECODER                Decoder log file (default: decoder.log)"
    echo "  LOG_PROXY                  Proxy log file (default: proxy.log)"
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

# Apply log level (override env.sh defaults if specified)
if [[ -n "$_LOG_LEVEL" ]]; then
    export VLLM_LOG_LEVEL="$_LOG_LEVEL"
    export LMCACHE_LOG_LEVEL="$_LOG_LEVEL"
fi

main
