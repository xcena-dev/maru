# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Contract tests for the maru_kv_ops placement kernels.

The kernel sources are vendored, so two things can drift without any test
failing: the binding surface the connectors call, and the provenance record
that the vendoring depends on. These tests pin both.

No kernel is launched here — a launch needs a GPU and a real paged cache, and
what these tests are for is catching drift on a vendor refresh. The C++ side of
that drift is caught by the compiler, since ``pybind.cpp`` names every argument.
"""

import pathlib

import pytest

pytest.importorskip("torch", reason="torch not installed")

import maru_kv_ops

_CSRC = pathlib.Path(maru_kv_ops.__file__).parent / "csrc"
_VENDOR_DOC = pathlib.Path(maru_kv_ops.__file__).parent / "VENDOR.md"

# Emitted by maru_vllm.kv_layout for the rank-5 paged tensors vLLM's Flash
# backends allocate. The kernels must be able to place all four.
_FORMATS_MARU_EMITS = (
    "NL_X_NB_TWO_BS_NH_HS",
    "NL_X_NB_TWO_NH_BS_HS",
    "NL_X_TWO_NB_BS_NH_HS",
    "NL_X_TWO_NB_NH_BS_HS",
)

_VENDORED_SOURCES = (
    "engine_kv_format.h",
    "kv_transfer_types.h",
    "mem_kernels.cu",
    "mem_kernels.cuh",
    "mp_mem_kernels.cu",
    "mp_mem_kernels.cuh",
)

requires_extension = pytest.mark.skipif(
    not maru_kv_ops.is_available(),
    reason=f"maru_kv_ops extension not built: {maru_kv_ops.import_error()}",
)


class TestPackagingContract:
    """Holds whether or not the extension was built."""

    def test_import_never_raises(self):
        """maru_vllm imports this package at load time on every host."""
        assert isinstance(maru_kv_ops.is_available(), bool)

    def test_unavailable_attribute_names_the_build_step(self):
        if maru_kv_ops.is_available():
            pytest.skip("extension is built; the guidance path is unreachable")
        with pytest.raises(AttributeError, match="nvcc"):
            _ = maru_kv_ops.multi_layer_kv_transfer

    def test_unknown_attribute_still_reports_the_module(self):
        with pytest.raises(AttributeError, match="has no attribute"):
            _ = maru_kv_ops.no_such_symbol

    def test_import_error_agrees_with_availability(self):
        assert (maru_kv_ops.import_error() is None) == maru_kv_ops.is_available()


class TestVendorProvenance:
    """Apache-2.0 obligations and the refresh record are part of the package."""

    def test_every_vendored_source_is_present(self):
        missing = [name for name in _VENDORED_SOURCES if not (_CSRC / name).is_file()]
        assert missing == []

    def test_vendored_sources_keep_their_spdx_header(self):
        without = [
            name
            for name in _VENDORED_SOURCES
            if "SPDX-License-Identifier: Apache-2.0"
            not in (_CSRC / name).read_text().splitlines()[0]
        ]
        assert without == []

    def test_vendor_doc_pins_a_revision(self):
        """A refresh that forgets to update the revision is a silent drift."""
        text = _VENDOR_DOC.read_text()
        assert "Apache-2.0" in text
        # A 40-character hex commit id, so "we copied from somewhere" cannot
        # pass as provenance.
        assert any(
            len(token.strip("`")) == 40
            and all(c in "0123456789abcdef" for c in token.strip("`"))
            for token in text.split()
        )


@requires_extension
class TestBindingSurface:
    """What the connectors call has to exist, with the names they pass."""

    @pytest.mark.parametrize(
        "name",
        [
            "multi_layer_kv_transfer",
            "single_layer_kv_transfer",
            "multi_layer_block_kv_transfer",
        ],
    )
    def test_entry_point_is_bound(self, name):
        assert callable(getattr(maru_kv_ops, name))

    @pytest.mark.parametrize("fmt", _FORMATS_MARU_EMITS)
    def test_format_maru_emits_is_bound(self, fmt):
        assert hasattr(maru_kv_ops.EngineKVFormat, fmt)

    def test_both_directions_are_bound(self):
        assert maru_kv_ops.TransferDirection.H2D is not None
        assert maru_kv_ops.TransferDirection.D2H is not None

    def test_single_layer_accepts_token_major(self):
        """The layer-overlap path passes token_major to select [2, T, H]."""
        assert "token_major" in maru_kv_ops.single_layer_kv_transfer.__doc__

    def test_multi_layer_accepts_the_dimension_keywords(self):
        doc = maru_kv_ops.multi_layer_kv_transfer.__doc__
        for keyword in ("block_size", "head_size", "skip_prefix_n_tokens"):
            assert keyword in doc

    def test_shape_desc_exposes_every_field(self):
        desc = maru_kv_ops.PageBufferShapeDesc()
        for field in (
            "kv_size",
            "nl",
            "nb",
            "bs",
            "nh",
            "hs",
            "element_size",
            "block_stride_elems",
        ):
            setattr(desc, field, 1)
            assert getattr(desc, field) == 1
