// SPDX-License-Identifier: Apache-2.0
//
// Python bindings for Maru's paged-KV placement kernels.
//
// The kernels themselves are vendored from LMCache (see ../VENDOR.md); this
// binding layer is Maru's own and exposes only the entry points the Maru
// connectors call. The unexposed kernels in the vendored translation units are
// still compiled — they are left untouched so the vendored sources stay
// byte-identical to their upstream revision, which is what makes a refresh a
// plain file copy.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <torch/extension.h>

#include "mem_kernels.cuh"
#include "mp_mem_kernels.cuh"

namespace py = pybind11;

PYBIND11_MODULE(_C, m) {
  m.doc() = "Maru paged-KV placement kernels";

  py::enum_<TransferDirection>(m, "TransferDirection")
      .value("H2D", TransferDirection::H2D)
      .value("D2H", TransferDirection::D2H)
      .export_values();

  // The full format set is bound even though Maru's layout detection emits
  // only the four rank-5 vLLM forms today: the enum is a plain header enum, a
  // value costs nothing, and binding all of them keeps a format added by a
  // future refresh usable without touching this file.
  py::enum_<EngineKVFormat>(m, "EngineKVFormat")
      .value("NB_NL_TWO_BS_NH_HS", EngineKVFormat::NB_NL_TWO_BS_NH_HS)
      .value("NL_X_TWO_NB_BS_NH_HS", EngineKVFormat::NL_X_TWO_NB_BS_NH_HS)
      .value("NL_X_NB_TWO_BS_NH_HS", EngineKVFormat::NL_X_NB_TWO_BS_NH_HS)
      .value("NL_X_NB_BS_HS", EngineKVFormat::NL_X_NB_BS_HS)
      .value("TWO_X_NL_X_NBBS_NH_HS", EngineKVFormat::TWO_X_NL_X_NBBS_NH_HS)
      .value("NL_X_NBBS_ONE_HS", EngineKVFormat::NL_X_NBBS_ONE_HS)
      .value("NL_X_TWO_NB_NH_BS_HS", EngineKVFormat::NL_X_TWO_NB_NH_BS_HS)
      .value("NL_X_NB_TWO_NH_BS_HS", EngineKVFormat::NL_X_NB_TWO_NH_BS_HS)
      .value("NB_NL_TWO_NH_BS_HS", EngineKVFormat::NB_NL_TWO_NH_BS_HS)
      .value("TWO_X_NL_X_NB_BS_NH_HS", EngineKVFormat::TWO_X_NL_X_NB_BS_NH_HS)
      .value("NL_X_NB_NH_BS_TWO_HS", EngineKVFormat::NL_X_NB_NH_BS_TWO_HS)
      .value("NL_X_NB_BS_NH_TWO_HS", EngineKVFormat::NL_X_NB_BS_NH_TWO_HS)
      .export_values();

  // Token-granular placement, every layer in one launch. Used by the packed
  // load (H2D) and the coalesced packed store (D2H).
  m.def("multi_layer_kv_transfer", &multi_layer_kv_transfer,
        py::arg("key_value"), py::arg("key_value_ptrs"),
        py::arg("slot_mapping"), py::arg("paged_memory_device"),
        py::arg("page_buffer_size"), py::arg("direction"),
        py::arg("engine_kv_format"), py::arg("block_size") = 0,
        py::arg("head_size") = 0, py::arg("skip_prefix_n_tokens") = 0,
        py::call_guard<py::gil_scoped_release>());

  // Token-granular placement for one layer. Used by the layer-overlap load,
  // which needs a per-layer completion event and therefore cannot hand the
  // whole slab to the multi-layer entry point. Reads the source pitch from the
  // tensor's stride, so a non-contiguous single-layer slice of a packed slab
  // is a valid argument.
  m.def("single_layer_kv_transfer", &single_layer_kv_transfer,
        py::arg("lmc_key_value_cache"), py::arg("vllm_key_value_cache"),
        py::arg("slot_mapping"), py::arg("direction"),
        py::arg("engine_kv_format"), py::arg("token_major") = false,
        py::call_guard<py::gil_scoped_release>());

  // Block-granular placement. Not called yet; bound so the per-block A/B can
  // be wired without rebuilding the extension. Its payload pointers are
  // dereferenced on the device, so the source must already be device-resident.
  m.def("multi_layer_block_kv_transfer", &multi_layer_block_kv_transfer,
        py::arg("paged_buffer_ptrs_tensor"), py::arg("lmcache_objects_ptrs"),
        py::arg("block_ids"), py::arg("device"), py::arg("direction"),
        py::arg("shape_desc"), py::arg("lmcache_chunk_size"),
        py::arg("engine_kv_format"), py::arg("skip_prefix_n_blocks"),
        py::call_guard<py::gil_scoped_release>());

  py::class_<PageBufferShapeDesc>(m, "PageBufferShapeDesc")
      .def(py::init<>())
      .def_readwrite("kv_size", &PageBufferShapeDesc::kv_size)
      .def_readwrite("nl", &PageBufferShapeDesc::nl)
      .def_readwrite("nb", &PageBufferShapeDesc::nb)
      .def_readwrite("bs", &PageBufferShapeDesc::bs)
      .def_readwrite("nh", &PageBufferShapeDesc::nh)
      .def_readwrite("hs", &PageBufferShapeDesc::hs)
      .def_readwrite("element_size", &PageBufferShapeDesc::element_size)
      .def_readwrite("block_stride_elems",
                     &PageBufferShapeDesc::block_stride_elems);
}
