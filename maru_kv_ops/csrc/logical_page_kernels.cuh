// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 XCENA Inc.
//
// Placement kernel for vLLM 0.29+'s logical KV page. Maru's own, not vendored
// (see ../VENDOR.md).

#pragma once

#include <cstdint>

#include <torch/all.h>

#include "kv_transfer_types.h"

// Where a logical KV page keeps its blocks, KV heads and tokens, in elements.
//
// vLLM 0.29 and later hand every attention layer the logical page
// [num_blocks, num_kv_heads, block_size, 2 * head_size], K then V in the last
// dimension, over whichever physical order the model's KVCacheLayout picks.
// The order shows only in the strides, so the kernel takes them as they are
// instead of naming the layout. The content dimension must be the innermost
// one (stride 1), which every KVCacheLayout keeps. num_heads is the page's
// KV head count, which the slab's row must match exactly.
struct LogicalPageFormat {
  int64_t block_stride;
  int64_t head_stride;
  int64_t token_stride;
  int64_t num_heads;
};

// Moves one chunk between a K/V-major slab and every layer's logical page.
//
// key_value: [2, num_layers, num_tokens, num_kv_heads * head_size], K plane
//   first, on the device or in host memory the device can reach (pinned or
//   CUDA-registered). The same bytes a rank-5 cache of vLLM < 0.29 produces.
// key_value_ptrs: [num_layers] int64 base pointers of the layers' logical
//   pages, on the device; every layer must share page_format.
// slot_mapping: [num_tokens] int64 slots (block * block_size + offset), on the
//   device; a negative slot skips its token.
// page_buffer_size: unused; kept so the call matches the vendored
//   multi_layer_kv_transfer, which this is bound beside.
void logical_page_kv_transfer(
    torch::Tensor& key_value, const torch::Tensor& key_value_ptrs,
    const torch::Tensor& slot_mapping, const torch::Device& paged_memory_device,
    const int page_buffer_size, const TransferDirection direction,
    const LogicalPageFormat& page_format, const int block_size,
    const int head_size, const int skip_prefix_n_tokens = 0);
