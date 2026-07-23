"""Setup script for Maru."""

from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            "maru_shm._cxl_flush",
            sources=["maru_shm/_cxl_flush.c"],
            # Best effort: without the extension, device_scanner falls back
            # to a no-op flush and logs a warning (single-host still works).
            optional=True,
        )
    ]
)
