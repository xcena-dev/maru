"""Setup script for Maru."""

import os
import shutil
import sys
from typing import Any

from setuptools import Extension, setup

_KV_OPS_SOURCES = [
    "maru_kv_ops/csrc/pybind.cpp",
    "maru_kv_ops/csrc/mem_kernels.cu",
    "maru_kv_ops/csrc/mp_mem_kernels.cu",
]


def _kv_ops_extension() -> Any | None:
    """Build the paged-KV placement extension, or None when tooling is absent.

    Maru's core is import-free of PyTorch and installs on hosts with no GPU
    toolchain, so this extension cannot be a hard build requirement. When
    PyTorch or nvcc is missing the extension is skipped and the connectors take
    their per-layer fallback; ``maru_kv_ops.is_available()`` reports which
    happened at runtime.

    ``optional=True`` on the extension is not enough on its own: importing
    ``torch.utils.cpp_extension`` to *describe* the build already requires
    PyTorch, so availability has to be settled before the object is created.

    Returns:
        A ``CUDAExtension`` when PyTorch and nvcc are both present, else None.
    """
    if os.environ.get("MARU_SKIP_KV_OPS"):
        print("maru: MARU_SKIP_KV_OPS set; skipping maru_kv_ops extension")
        return None
    try:
        from torch.utils.cpp_extension import CUDAExtension
    except ImportError:
        print(
            "maru: PyTorch not importable; skipping the maru_kv_ops extension. "
            "The vLLM connector will use its per-layer fallback copy path.",
            file=sys.stderr,
        )
        return None
    if shutil.which("nvcc") is None and not os.environ.get("CUDA_HOME"):
        print(
            "maru: nvcc not found; skipping the maru_kv_ops extension. "
            "The vLLM connector will use its per-layer fallback copy path.",
            file=sys.stderr,
        )
        return None
    return CUDAExtension(
        name="maru_kv_ops._C",
        sources=_KV_OPS_SOURCES,
        extra_compile_args={
            # The vendored translation units are compiled as-is; -O3 matches
            # how LMCache builds them.
            "cxx": ["-O3"],
            "nvcc": ["-O3"],
        },
        # A compile failure must not brick `pip install`. The runtime side is
        # where an unbuilt extension is made loud: the connector logs a warning
        # naming this build step before it falls back.
        optional=True,
    )


def _build_config() -> dict[str, Any]:
    """Assemble ext_modules and the matching build_ext command class.

    ``CUDAExtension`` needs PyTorch's ``BuildExtension`` to compile; the plain
    ``distutils`` build_ext does not know ``.cu``. The command class is
    therefore only overridden when the extension is actually being built.

    Returns:
        Keyword arguments for ``setup()``.
    """
    ext_modules: list[Any] = [
        Extension(
            "maru_shm._cxl_flush",
            sources=["maru_shm/_cxl_flush.c"],
            # Best effort: without the extension, device_scanner falls back
            # to a no-op flush and logs a warning (single-host still works).
            optional=True,
        )
    ]
    kv_ops = _kv_ops_extension()
    if kv_ops is None:
        return {"ext_modules": ext_modules}

    from torch.utils.cpp_extension import BuildExtension

    ext_modules.append(kv_ops)
    return {"ext_modules": ext_modules, "cmdclass": {"build_ext": BuildExtension}}


setup(**_build_config())
