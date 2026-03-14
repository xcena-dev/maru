#!/bin/bash

export MODEL=${MODEL:-"Qwen/Qwen2.5-0.5B"}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}
export SGLANG_LOG_LEVEL=${SGLANG_LOG_LEVEL:-info}
export MARU_LOG_LEVEL=${MARU_LOG_LEVEL:-DEBUG}
# export MARU_LOG_LEVEL=${MARU_LOG_LEVEL:-INFO}

# Ports: UID-based dynamic allocation (avoids collision in multi-user env)
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
export SGLANG_INST1_PORT=${SGLANG_INST1_PORT:-$((20000 + $(id -u) + 10))}
export SGLANG_INST2_PORT=${SGLANG_INST2_PORT:-$((20000 + $(id -u) + 11))}

export INST1_GPU=${INST1_GPU:-0}
export INST2_GPU=${INST2_GPU:-1}
export MARU_POOL_SIZE=${MARU_POOL_SIZE:-4G}
