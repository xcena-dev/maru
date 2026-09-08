"""Setup script for Maru."""

import os
import sys
from typing import Any

from setuptools import Extension, setup

_KV_OPS_NAME = "maru_kv_ops._C"
_KV_OPS_SOURCES = [
    "maru_kv_ops/csrc/pybind.cpp",
    "maru_kv_ops/csrc/mem_kernels.cu",
    "maru_kv_ops/csrc/mp_mem_kernels.cu",
]
_FALLBACK_NOTE = (
    "The vLLM connector will use its per-layer fallback copy path, which is "
    "materially slower. Rerun the install on a host with PyTorch and the CUDA "
    "toolkit, without build isolation (install.sh does this for you)."
)


def _nvcc_path() -> str | None:
    """Locate nvcc the way ``torch.utils.cpp_extension`` will.

    PyTorch resolves the toolkit root once at import into ``CUDA_HOME`` and
    then requires ``$CUDA_HOME/bin/nvcc`` to exist; a ``CUDA_HOME`` pointing
    at a directory without nvcc makes its preflight raise rather than skip.
    Asking PyTorch for the same root is what keeps this gate and that
    preflight from disagreeing.

    Returns:
        Absolute path to nvcc, or None when PyTorch cannot find a toolkit.
    """
    from torch.utils import cpp_extension

    cuda_home = cpp_extension.CUDA_HOME
    if not cuda_home:
        return None
    nvcc = os.path.join(cuda_home, "bin", "nvcc.exe" if os.name == "nt" else "nvcc")
    return nvcc if os.path.exists(nvcc) else None


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
            "Note that a PEP 517 isolated build never sees PyTorch even when "
            f"the target environment has it. {_FALLBACK_NOTE}",
            file=sys.stderr,
        )
        return None
    if _nvcc_path() is None:
        print(
            "maru: no CUDA toolkit (nvcc) found where PyTorch looks for it; "
            f"skipping the maru_kv_ops extension. {_FALLBACK_NOTE}",
            file=sys.stderr,
        )
        return None
    return CUDAExtension(
        name=_KV_OPS_NAME,
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


def _optional_kv_ops_build(base: type) -> type:
    """Wrap ``BuildExtension`` so a refused build drops only this extension.

    ``optional=True`` covers a compile that fails once it has started:
    setuptools catches per-extension compiler errors and moves on. PyTorch's
    ``build_extensions`` opens with a CUDA and host-compiler preflight, which
    sits *before* that loop, and every way it can disagree raises out of the
    whole command instead — an nvcc major differing from the one PyTorch was
    built against, or a host compiler outside the bounds the toolkit supports.
    Maru's core still has to install on such a host, so the preflight is run
    here first and a refusal drops the extension rather than the install.

    Args:
        base: PyTorch's ``BuildExtension`` command class.

    Returns:
        A subclass of it that treats the KV-ops extension as droppable.
    """
    from setuptools.command.build_ext import build_ext as _plain_build_ext

    class _OptionalKVOpsBuild(base):  # type: ignore[misc, valid-type]
        def build_extensions(self) -> None:
            refusal = self._kv_ops_refusal()
            if refusal is None:
                super().build_extensions()
                return
            print(
                f"maru: cannot build the maru_kv_ops extension ({refusal}). "
                f"{_FALLBACK_NOTE}",
                file=sys.stderr,
            )
            self.extensions = [
                ext for ext in self._declared_extensions() if ext.name != _KV_OPS_NAME
            ]
            # Only plain C is left, so the base command builds it — and it
            # does not re-run the preflight that just refused.
            _plain_build_ext.build_extensions(self)

        def _declared_extensions(self) -> list[Any]:
            """Return the extensions this command was handed.

            Wrapped because the base command class arrives as a runtime
            argument, so mypy cannot resolve the attribute it declares.

            Returns:
                The current extension list.
            """
            return list(self.extensions)  # type: ignore[has-type]

        def _kv_ops_refusal(self) -> Exception | None:
            """Ask PyTorch's own checks whether this build can start.

            Returns:
                The exception the preflight raises, or None when it passes,
                when the extension is not being built anyway, or when the
                checks are not where this expects them (a PyTorch refactor
                then leaves behaviour exactly as it is without them).
            """
            if not any(ext.name == _KV_OPS_NAME for ext in self._declared_extensions()):
                return None
            from torch.utils import cpp_extension

            check_cuda_version = getattr(cpp_extension, "_check_cuda_version", None)
            check_abi = getattr(self, "_check_abi", None)
            if check_cuda_version is None or check_abi is None:
                return None
            try:
                check_cuda_version(*check_abi())
            except Exception as error:
                return error
            return None

    return _OptionalKVOpsBuild


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
    return {
        "ext_modules": ext_modules,
        "cmdclass": {"build_ext": _optional_kv_ops_build(BuildExtension)},
    }


setup(**_build_config())
