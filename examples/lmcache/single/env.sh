#!/bin/bash

export VLLM_LOG_LEVEL=${VLLM_LOG_LEVEL:-DEBUG}
export LMCACHE_LOG_LEVEL=${LMCACHE_LOG_LEVEL:-INFO}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}

# Port base configuration
# Uses user ID to avoid port conflicts between users on shared machines
export LMCACHE_PORT_BASE=${LMCACHE_PORT_BASE:-$((12000 + $(id -u)))}

# Single instance port
export LMCACHE_INST_PORT=${LMCACHE_INST_PORT:-$((LMCACHE_PORT_BASE + 20))}

# Maru Server port
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
