# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Contract tests for the maru_kv_ops placement kernels.

The kernel sources are vendored, so three things can drift without any test
failing: the binding surface the connectors call, the set of formats the
kernels dispatch, and the provenance record that the vendoring depends on.
These tests pin all three.

No kernel is launched here — a launch needs a GPU and a real paged cache, and
what these tests are for is catching drift on a vendor refresh. The C++ side of
the binding drift is caught by the compiler, since ``pybind.cpp`` names every
argument.

Everything except the binding surface reads files, so it runs on a host with
neither PyTorch nor a CUDA toolkit — which is where CI runs, and therefore the
only place these guards can fire before a review.
"""

import hashlib
import importlib.util
import pathlib
import re

import pytest

import maru_kv_ops

_PACKAGE = pathlib.Path(maru_kv_ops.__file__).parent
_CSRC = _PACKAGE / "csrc"
_VENDOR_DOC = _PACKAGE / "VENDOR.md"

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


def _kv_layout_source() -> str:
    """Read ``maru_vllm/kv_layout.py`` without importing it.

    Importing ``maru_vllm`` pulls in PyTorch, which is exactly what these tests
    avoid needing. ``find_spec`` on a top-level package resolves its location
    without executing it.

    Returns:
        The module's source text.
    """
    spec = importlib.util.find_spec("maru_vllm")
    assert spec is not None and spec.origin is not None, "maru_vllm not installed"
    return (pathlib.Path(spec.origin).parent / "kv_layout.py").read_text()


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
        text = _VENDOR_DOC.read_text()
        assert "Apache-2.0" in text
        # A 40-character hex commit id, so "we copied from somewhere" cannot
        # pass as provenance.
        assert any(
            len(token.strip("`")) == 40
            and all(c in "0123456789abcdef" for c in token.strip("`"))
            for token in text.split()
        )

    def test_vendor_doc_records_every_copied_file(self):
        recorded = self._recorded_digests()
        assert sorted(recorded) == sorted(_VENDORED_SOURCES)

    @pytest.mark.parametrize("name", _VENDORED_SOURCES)
    def test_copied_file_matches_its_recorded_digest(self, name):
        """New bytes under an old revision id is the drift that matters.

        The revision line alone cannot catch it: a refresh that copies the
        files and forgets the table leaves a record that reads as authoritative
        and is wrong. Comparing the bytes is what makes the record a claim the
        suite checks rather than a comment.
        """
        recorded = self._recorded_digests()
        actual = hashlib.sha256((_CSRC / name).read_bytes()).hexdigest()
        assert actual == recorded[name], (
            f"{name} does not match the digest in VENDOR.md. Refresh the table "
            "(and the revision above it) with: cd maru_kv_ops/csrc && "
            "sha256sum *.h *.cu *.cuh"
        )

    @staticmethod
    def _recorded_digests() -> dict[str, str]:
        """Parse the SHA-256 table out of VENDOR.md.

        Returns:
            Vendored file base name -> the digest recorded for it.
        """
        pattern = re.compile(r"`csrc/([\w.]+)`\s*\|\s*`([0-9a-f]{64})`")
        return dict(pattern.findall(_VENDOR_DOC.read_text()))


class TestFormatDispatch:
    """Every format the connector can hand a kernel has to reach a case.

    The connector's gate accepts any format the ``EngineKVFormat`` enum names,
    because it was written for ``multi_layer_kv_transfer``. The layer-overlap
    load calls ``single_layer_kv_transfer``, whose switch covers a narrower
    set and raises on the rest. The two agree today; a vendor refresh or a new
    paged layout is where they would stop agreeing, and the failure lands
    inside a CUDA stream at serving time rather than here.
    """

    @staticmethod
    def _single_layer_cases() -> set[str]:
        """Formats ``single_layer_kv_transfer`` dispatches, read from its switch."""
        source = (_CSRC / "mem_kernels.cu").read_text()
        cases = re.findall(
            r"LAUNCH_SINGLE_LAYER_KERNEL\(EngineKVFormat::(\w+)\)", source
        )
        assert cases, "the single-layer switch moved; this test needs updating"
        return set(cases)

    @pytest.mark.parametrize("fmt", _FORMATS_MARU_EMITS)
    def test_single_layer_transfer_dispatches_the_format(self, fmt):
        assert fmt in self._single_layer_cases()

    def test_the_recorded_format_list_is_what_kv_layout_emits(self):
        """Keeps the list above honest when a layout is added or renamed.

        Format names are the only screaming-case string literals in
        ``kv_layout.py``, so reading them out of the source keeps this list
        tied to the module without importing it.
        """
        emitted = set(re.findall(r'"([A-Z][A-Z0-9_]{5,})"', _kv_layout_source()))
        assert emitted == set(_FORMATS_MARU_EMITS)


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
