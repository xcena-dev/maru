// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 XCENA Inc.
//
// Placement kernel for vLLM 0.29+'s logical KV page. Maru's own, not vendored
// (see ../VENDOR.md).
//
// The vendored kernels pick their addressing from an EngineKVFormat. None of
// those describes this page for Maru's slab: the *_TWO_HS and *_CS formats
// move each head's K|V content as one row, which would store head-interleaved
// bytes, while Maru stores a chunk K/V-major (all K, then all V), the bytes a
// rank-5 cache of vLLM < 0.29 and the per-layer fallback produce. Keeping one
// slab format is what lets those paths and this kernel read each other's
// objects. So this kernel splits each head's content into its K and V halves
// and addresses the page by its strides.

#include "logical_page_kernels.cuh"

#include <ATen/cuda/CUDAContext.h>
#include <c10/cuda/CUDAGuard.h>
#include <cuda_runtime.h>

#include <algorithm>

namespace {

// One thread block per (token, layer, K or V), the launch shape of the
// vendored multi-layer kernel. Threads walk the token's row in the slab, which
// is contiguous, and land each head's run of head_size elements in the page.
template <typename T, bool TO_SLAB>
__global__ void logical_page_kv_transfer_kernel(
    T* __restrict__ slab, T* const* __restrict__ pages,
    const int64_t* __restrict__ slot_mapping, const int row, const int head,
    const int num_tokens, const int num_layers, const int block_size,
    const int64_t block_stride, const int64_t head_stride,
    const int64_t token_stride, const int skip_prefix_n_tokens) {
  const int token = blockIdx.x + skip_prefix_n_tokens;
  const int layer = blockIdx.y;
  const int k_or_v = blockIdx.z;
  const int64_t slot = slot_mapping[token];
  if (slot < 0) {
    return;
  }
  // K is the first half of each head's content, V the second.
  T* const page = pages[layer] + (slot / block_size) * block_stride +
                  (slot % block_size) * token_stride + k_or_v * head;
  T* const chunk =
      slab +
      ((static_cast<int64_t>(k_or_v) * num_layers + layer) * num_tokens +
       token) *
          row;
  for (int i = threadIdx.x; i < row; i += blockDim.x) {
    T* const paged = page + (i / head) * head_stride + (i % head);
    if constexpr (TO_SLAB) {
      chunk[i] = *paged;
    } else {
      *paged = chunk[i];
    }
  }
}

// A device address for a tensor the kernel reads or writes.
template <typename T>
T* device_ptr(const torch::Tensor& tensor, const char* what) {
  if (tensor.is_cuda()) {
    return static_cast<T*>(tensor.data_ptr());
  }
  TORCH_CHECK(tensor.is_cpu(), what,
              " must be on a CUDA device or in host memory");
  void* ptr = nullptr;
  if (cudaHostGetDevicePointer(&ptr, tensor.data_ptr(), 0) != cudaSuccess) {
    cudaGetLastError();  // not sticky; keep it out of the launch check below
    TORCH_CHECK(false, what,
                " is host memory the device cannot reach (neither pinned "
                "nor CUDA-registered)");
  }
  return static_cast<T*>(ptr);
}

template <typename T>
void launch(torch::Tensor& key_value, const torch::Tensor& key_value_ptrs,
            const torch::Tensor& slot_mapping, const TransferDirection direction,
            const LogicalPageFormat& page_format, const int block_size,
            const int head_size, const int skip_prefix_n_tokens,
            const int width) {
  const int elements = width / static_cast<int>(key_value.element_size());
  const int num_layers = static_cast<int>(key_value.size(1));
  const int num_tokens = static_cast<int>(key_value.size(2));
  const int row = static_cast<int>(key_value.size(3)) / elements;
  const int head = head_size / elements;

  T* const slab = device_ptr<T>(key_value, "key_value");
  T* const* const pages =
      device_ptr<T* const>(key_value_ptrs, "key_value_ptrs");
  const int64_t* const slots =
      device_ptr<const int64_t>(slot_mapping, "slot_mapping");

  const dim3 grid(num_tokens - skip_prefix_n_tokens, num_layers, 2);
  const dim3 block(std::min(row, 128));
  const cudaStream_t stream = at::cuda::getCurrentCUDAStream();
  const int64_t block_stride = page_format.block_stride / elements;
  const int64_t head_stride = page_format.head_stride / elements;
  const int64_t token_stride = page_format.token_stride / elements;
  if (direction == TransferDirection::D2H) {
    logical_page_kv_transfer_kernel<T, true><<<grid, block, 0, stream>>>(
        slab, pages, slots, row, head, num_tokens, num_layers, block_size,
        block_stride, head_stride, token_stride, skip_prefix_n_tokens);
  } else {
    logical_page_kv_transfer_kernel<T, false><<<grid, block, 0, stream>>>(
        slab, pages, slots, row, head, num_tokens, num_layers, block_size,
        block_stride, head_stride, token_stride, skip_prefix_n_tokens);
  }
  C10_CUDA_KERNEL_LAUNCH_CHECK();
}

}  // namespace

void logical_page_kv_transfer(
    torch::Tensor& key_value, const torch::Tensor& key_value_ptrs,
    const torch::Tensor& slot_mapping, const torch::Device& paged_memory_device,
    const int page_buffer_size, const TransferDirection direction,
    const LogicalPageFormat& page_format, const int block_size,
    const int head_size, const int skip_prefix_n_tokens) {
  (void)page_buffer_size;
  TORCH_CHECK(key_value.dim() == 4 && key_value.size(0) == 2,
              "key_value must be [2, num_layers, num_tokens, row]");
  TORCH_CHECK(key_value.is_contiguous(), "key_value must be contiguous");
  TORCH_CHECK(key_value_ptrs.scalar_type() == torch::kInt64 &&
                  key_value_ptrs.numel() == key_value.size(1),
              "key_value_ptrs must hold one int64 pointer per layer");
  TORCH_CHECK(slot_mapping.scalar_type() == torch::kInt64 &&
                  slot_mapping.numel() == key_value.size(2),
              "slot_mapping must hold one int64 slot per token");
  TORCH_CHECK(block_size > 0, "block_size must be positive");
  TORCH_CHECK(head_size > 0 &&
                  key_value.size(3) == page_format.num_heads * head_size,
              "head_size must be positive and the row must be num_heads "
              "heads wide");
  TORCH_CHECK(page_format.block_stride > 0 && page_format.head_stride > 0 &&
                  page_format.token_stride > 0,
              "page strides must be positive");
  TORCH_CHECK(
      skip_prefix_n_tokens >= 0 && skip_prefix_n_tokens < key_value.size(2),
      "skip_prefix_n_tokens must leave at least one token");

  // The widest access that every head run, stride and the slab itself are
  // aligned to. The pages' base pointers are the caller's to align (the
  // connector checks them against 16 bytes).
  const int64_t elem = key_value.element_size();
  const uintptr_t base = reinterpret_cast<uintptr_t>(key_value.data_ptr());
  int width = 16;
  while (width > elem &&
         ((head_size * elem) % width || (page_format.block_stride * elem) % width ||
          (page_format.head_stride * elem) % width ||
          (page_format.token_stride * elem) % width || base % width)) {
    width /= 2;
  }

  const at::cuda::OptionalCUDAGuard device_guard(paged_memory_device);
  switch (width) {
    case 16:
      launch<int4>(key_value, key_value_ptrs, slot_mapping, direction,
                   page_format, block_size, head_size, skip_prefix_n_tokens,
                   width);
      break;
    case 8:
      launch<int2>(key_value, key_value_ptrs, slot_mapping, direction,
                   page_format, block_size, head_size, skip_prefix_n_tokens,
                   width);
      break;
    case 4:
      launch<int32_t>(key_value, key_value_ptrs, slot_mapping, direction,
                      page_format, block_size, head_size, skip_prefix_n_tokens,
                      width);
      break;
    case 2:
      launch<int16_t>(key_value, key_value_ptrs, slot_mapping, direction,
                      page_format, block_size, head_size, skip_prefix_n_tokens,
                      width);
      break;
    default:
      launch<int8_t>(key_value, key_value_ptrs, slot_mapping, direction,
                     page_format, block_size, head_size, skip_prefix_n_tokens,
                     width);
  }
}
