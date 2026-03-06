# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Shared logging setup for Maru packages."""

import logging
import os

# Track which loggers have been configured to avoid duplicate setup
_configured_loggers: set[str] = set()


def setup_package_logging(package_name: str) -> logging.Logger:
    """Setup logging for a Maru package.

    Args:
        package_name: Name of the package (e.g., "maru", "maru_handler")

    Returns:
        Configured logger for the package
    """
    if package_name in _configured_loggers:
        return logging.getLogger(package_name)

    # MARU_LOG_LEVEL takes precedence; fall back to LMCACHE_LOG_LEVEL for
    # backward compatibility with LMCache deployments (default: INFO)
    log_level_str = os.getenv(
        "MARU_LOG_LEVEL", os.getenv("LMCACHE_LOG_LEVEL", "INFO")
    ).upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger(package_name)
    logger.setLevel(log_level)

    # Only add handler if none exists
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(log_level)
        formatter = logging.Formatter(
            "[%(asctime)s,%(msecs)03.0f] maru %(levelname)s: %(message)s "
            "(%(filename)s:%(lineno)d:%(name)s)",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    _configured_loggers.add(package_name)
    return logger
