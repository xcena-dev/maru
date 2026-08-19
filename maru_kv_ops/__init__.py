# SPDX-License-Identifier: Apache-2.0
"""Paged-KV placement kernels for the Maru serving-engine connectors.

Moving a KV cache between a Maru CXL object and a serving engine's paged KV
cache is not a copy: the engine scatters a request's tokens across fixed-size
blocks, so every token's destination address has to be resolved from a mapping
table. These kernels do that resolution on the device.

The extension is built only where CUDA tooling is present, so importing this
package always succeeds and ``is_available()`` reports whether the kernels can
actually be called. Callers are expected to branch on it and keep whatever
fallback they have — see ``MaruKVConnector._packed_load_kernel_ctx``.

Attributes:
    TransferDirection: H2D (object to paged cache) or D2H (paged cache to
        object). Present only when the extension is available.
    EngineKVFormat: Paged-cache axis order the kernels index with. Present only
        when the extension is available.
"""

# Standard
from typing import Any

__all__ = [
    "EngineKVFormat",
    "PageBufferShapeDesc",
    "TransferDirection",
    "import_error",
    "is_available",
    "multi_layer_block_kv_transfer",
    "multi_layer_kv_transfer",
    "single_layer_kv_transfer",
]

_IMPORT_ERROR: Exception | None = None

try:
    # Third Party
    # The extension links against libtorch, whose shared objects are found
    # through the torch package's own loader. Importing it first is what puts
    # libc10 and libtorch_cuda on the loader path; without this the extension
    # import fails with a missing-libc10 OSError even when it built fine.
    import torch  # noqa: F401

    # Local
    from maru_kv_ops import _C  # type: ignore[attr-defined]
except Exception as exc:  # pragma: no cover - depends on build environment
    # Broad on purpose: a mismatched torch ABI raises neither ImportError nor
    # any other single type, and an unusable extension must degrade to the
    # caller's fallback rather than break the import of maru_vllm.
    _C = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    EngineKVFormat = _C.EngineKVFormat
    PageBufferShapeDesc = _C.PageBufferShapeDesc
    TransferDirection = _C.TransferDirection
    multi_layer_block_kv_transfer = _C.multi_layer_block_kv_transfer
    multi_layer_kv_transfer = _C.multi_layer_kv_transfer
    single_layer_kv_transfer = _C.single_layer_kv_transfer


def is_available() -> bool:
    """Report whether the compiled kernels can be called.

    Returns:
        True when the extension imported, so the module-level kernel and enum
        attributes exist. False when it did not, in which case those attributes
        are absent and ``import_error()`` explains why.
    """
    return _C is not None


def import_error() -> Exception | None:
    """Return why the extension is unavailable.

    Returns:
        The exception raised while importing the extension, or None when the
        extension imported successfully.
    """
    return _IMPORT_ERROR


def __getattr__(name: str) -> Any:
    """Explain an unavailable extension instead of raising a bare NameError.

    Only reached for the kernel and enum names, which are bound at import time
    when the extension is available.

    Args:
        name: Attribute being looked up.

    Returns:
        Never returns; always raises.

    Raises:
        AttributeError: Always, naming the build requirement for kernel and
            enum attributes and the module for anything else.
    """
    if name in __all__:
        raise AttributeError(
            f"maru_kv_ops.{name} is unavailable: the CUDA extension was not "
            f"built or failed to import ({_IMPORT_ERROR}). Reinstall maru on a "
            "host with the CUDA toolkit (nvcc) and PyTorch to build it."
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
