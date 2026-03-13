#!/bin/bash

export MODEL=${MODEL:-"Qwen/Qwen2.5-0.5B"}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}
export SGLANG_LOG_LEVEL=${SGLANG_LOG_LEVEL:-info}
export MARU_LOG_LEVEL=${MARU_LOG_LEVEL:-INFO}

# SGLang server port
export SGLANG_PORT_BASE=${SGLANG_PORT_BASE:-$((20000 + $(id -u)))}
export SGLANG_PORT=${SGLANG_PORT:-$((SGLANG_PORT_BASE + 10))}

# Maru Server port (for L3 backend)
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
