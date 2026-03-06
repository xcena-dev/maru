# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for logging_setup module."""

import logging
from io import StringIO

import pytest

from maru_common import logging_setup
from maru_common.logging_setup import setup_package_logging


@pytest.fixture(autouse=True)
def reset_logging_state():
    """Reset logging state before and after each test."""
    # Store original state
    original_configured = logging_setup._configured_loggers.copy()

    yield

    # Cleanup: remove test loggers and reset state
    logging_setup._configured_loggers.clear()
    logging_setup._configured_loggers.update(original_configured)

    # Remove any test loggers we created
    for name in list(logging.Logger.manager.loggerDict.keys()):
        if name.startswith("test_"):
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.setLevel(logging.NOTSET)


class TestSetupPackageLogging:
    """Test setup_package_logging function."""

    def test_returns_logger(self):
        """Should return a Logger instance."""
        logger = setup_package_logging("test_pkg_1")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_pkg_1"

    def test_sets_log_level_default_info(self, monkeypatch):
        """Default log level should be INFO."""
        monkeypatch.delenv("MARU_LOG_LEVEL", raising=False)
        monkeypatch.delenv("LMCACHE_LOG_LEVEL", raising=False)
        # Clear from configured set to force re-setup
        logging_setup._configured_loggers.discard("test_pkg_2")

        logger = setup_package_logging("test_pkg_2")
        assert logger.level == logging.INFO

    def test_sets_log_level_from_env(self, monkeypatch):
        """Should respect LMCACHE_LOG_LEVEL environment variable."""
        monkeypatch.setenv("LMCACHE_LOG_LEVEL", "DEBUG")
        logger = setup_package_logging("test_pkg_debug")
        assert logger.level == logging.DEBUG

    def test_maru_log_level_takes_precedence(self, monkeypatch):
        """MARU_LOG_LEVEL should take precedence over LMCACHE_LOG_LEVEL."""
        monkeypatch.setenv("MARU_LOG_LEVEL", "WARNING")
        monkeypatch.setenv("LMCACHE_LOG_LEVEL", "DEBUG")
        logger = setup_package_logging("test_pkg_maru_precedence")
        assert logger.level == logging.WARNING

    def test_sets_log_level_warning(self, monkeypatch):
        """Should handle WARNING level."""
        monkeypatch.setenv("LMCACHE_LOG_LEVEL", "WARNING")
        logger = setup_package_logging("test_pkg_warning")
        assert logger.level == logging.WARNING

    def test_case_insensitive_log_level(self, monkeypatch):
        """Log level should be case insensitive."""
        monkeypatch.setenv("LMCACHE_LOG_LEVEL", "debug")
        logger = setup_package_logging("test_pkg_case")
        assert logger.level == logging.DEBUG

    def test_invalid_log_level_defaults_to_info(self, monkeypatch):
        """Invalid log level should default to INFO."""
        monkeypatch.setenv("LMCACHE_LOG_LEVEL", "INVALID_LEVEL")
        logger = setup_package_logging("test_pkg_invalid")
        assert logger.level == logging.INFO

    def test_adds_stream_handler(self):
        """Should add a StreamHandler."""
        logger = setup_package_logging("test_pkg_handler")
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_no_duplicate_handlers(self):
        """Calling twice should not add duplicate handlers."""
        logger1 = setup_package_logging("test_pkg_dup")
        handler_count_1 = len(logger1.handlers)

        # Force re-call by removing from configured set but keeping handler
        logging_setup._configured_loggers.discard("test_pkg_dup")
        logger2 = setup_package_logging("test_pkg_dup")

        assert logger1 is logger2
        assert len(logger2.handlers) == handler_count_1

    def test_no_propagation(self):
        """Logger should not propagate to root."""
        logger = setup_package_logging("test_pkg_prop")
        assert logger.propagate is False

    def test_idempotent_when_already_configured(self):
        """Should return cached logger when already configured."""
        logger1 = setup_package_logging("test_pkg_idem")
        logger2 = setup_package_logging("test_pkg_idem")
        assert logger1 is logger2

    def test_tracks_configured_loggers(self):
        """Should track configured loggers in _configured_loggers set."""
        setup_package_logging("test_pkg_track")
        assert "test_pkg_track" in logging_setup._configured_loggers


class TestLogOutput:
    """Test actual log output."""

    def test_log_format(self):
        """Should use the expected log format."""
        logger = setup_package_logging("test_pkg_format")

        # Capture log output
        stream = StringIO()
        logger.handlers[0].stream = stream

        logger.info("test message")
        output = stream.getvalue()

        # Check format components
        assert "maru INFO:" in output
        assert "test message" in output
        assert "test_logging_setup.py:" in output
        assert "test_pkg_format" in output

    def test_child_logger_inherits(self):
        """Child loggers should inherit parent's configuration."""
        parent = setup_package_logging("test_pkg_parent")
        child = logging.getLogger("test_pkg_parent.child")

        # Child should inherit level (effective level)
        assert child.getEffectiveLevel() == parent.level

    def test_debug_logs_when_debug_level(self, monkeypatch):
        """DEBUG messages should appear when level is DEBUG."""
        monkeypatch.setenv("LMCACHE_LOG_LEVEL", "DEBUG")
        logger = setup_package_logging("test_pkg_dbg_out")

        stream = StringIO()
        logger.handlers[0].stream = stream

        logger.debug("debug message")
        output = stream.getvalue()

        assert "debug message" in output

    def test_debug_logs_hidden_when_info_level(self, monkeypatch):
        """DEBUG messages should be hidden when level is INFO."""
        monkeypatch.delenv("MARU_LOG_LEVEL", raising=False)
        monkeypatch.delenv("LMCACHE_LOG_LEVEL", raising=False)
        logger = setup_package_logging("test_pkg_info_hide")

        stream = StringIO()
        logger.handlers[0].stream = stream

        logger.debug("debug message")
        output = stream.getvalue()

        assert "debug message" not in output
