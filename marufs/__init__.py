"""marufs - Python interface to marufs kernel filesystem for CXL shared memory."""

from maru_common.logging_setup import setup_package_logging

setup_package_logging("marufs")

from .client import MarufsClient  # noqa: E402

__all__ = [
    "MarufsClient",
]
