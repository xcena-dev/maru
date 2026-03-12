#!/bin/bash
# Environment variables for Maru-vLLM direct integration example

export VLLM_LOG_LEVEL=${VLLM_LOG_LEVEL:-DEBUG}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}

# Port configuration (user ID based to avoid conflicts on shared machines)
export MARU_PORT_BASE=${MARU_PORT_BASE:-$((12000 + $(id -u)))}
export MARU_INST1_PORT=${MARU_INST1_PORT:-$((MARU_PORT_BASE + 10))}
export MARU_INST2_PORT=${MARU_INST2_PORT:-$((MARU_PORT_BASE + 11))}

# Maru Server
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
export MARU_SERVER_URL="tcp://localhost:${MARU_SERVER_PORT}"

# Maru KV connector settings
export MARU_POOL_SIZE=${MARU_POOL_SIZE:-"4G"}
export MARU_KV_CHUNK_TOKENS=${MARU_KV_CHUNK_TOKENS:-256}
