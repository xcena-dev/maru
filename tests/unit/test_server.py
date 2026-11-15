# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for server.py CLI utilities (setup_logging, main)."""

import logging
import signal
from unittest.mock import MagicMock, patch

import pytest

from maru_server.server import main, setup_logging


class TestSetupLogging:
    """Test setup_logging function."""

    def test_setup_logging_info(self):
        """setup_logging('INFO') sets package logger to INFO level."""
        setup_logging("INFO")
        assert logging.getLogger("maru_server").level == logging.INFO

    def test_setup_logging_debug(self):
        """setup_logging('debug') works with lowercase input."""
        setup_logging("debug")
        assert logging.getLogger("maru_server").level == logging.DEBUG

    def test_setup_logging_warning(self):
        """setup_logging('WARNING') sets package logger to WARNING level."""
        setup_logging("WARNING")
        assert logging.getLogger("maru_server").level == logging.WARNING

    def test_setup_logging_error(self):
        """setup_logging('ERROR') sets package logger to ERROR level."""
        setup_logging("ERROR")
        assert logging.getLogger("maru_server").level == logging.ERROR


class TestMain:
    """Test main() entry point."""

    def test_main_default_args(self):
        """main() with default args creates server and starts it."""
        with (
            patch("sys.argv", ["maru-server"]),
            patch("maru_server.server.setup_logging") as mock_setup,
            patch("maru_server.server.MaruServer") as mock_server_cls,
            patch("maru_server.rpc_server.RpcServer") as mock_rpc_server_cls,
            patch("signal.signal") as mock_signal,
        ):
            mock_server_instance = MagicMock()
            mock_server_cls.return_value = mock_server_instance
            mock_rpc = MagicMock()
            mock_rpc_server_cls.return_value = mock_rpc

            main()

            # Verify setup_logging was called with default
            mock_setup.assert_called_once_with("INFO")

            # Verify server created
            mock_server_cls.assert_called_once()

            # Verify RPC server created with defaults
            mock_rpc_server_cls.assert_called_once_with(
                mock_server_instance, host="127.0.0.1", port=5555
            )

            # Verify signal handlers registered
            assert mock_signal.call_count == 2

            # Verify server started
            mock_rpc.start.assert_called_once()

    def test_main_custom_args(self):
        """main() with custom args passes them correctly."""
        with (
            patch(
                "sys.argv",
                [
                    "maru-server",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "9999",
                    "--log-level",
                    "DEBUG",
                ],
            ),
            patch("maru_server.server.setup_logging") as mock_setup,
            patch("maru_server.server.MaruServer") as mock_server_cls,
            patch("maru_server.rpc_server.RpcServer") as mock_rpc_server_cls,
            patch("signal.signal"),
        ):
            mock_server_instance = MagicMock()
            mock_server_cls.return_value = mock_server_instance
            mock_rpc = MagicMock()
            mock_rpc_server_cls.return_value = mock_rpc

            main()

            # Verify custom log level
            mock_setup.assert_called_once_with("DEBUG")

            # Verify custom host/port
            mock_rpc_server_cls.assert_called_once_with(
                mock_server_instance, host="127.0.0.1", port=9999
            )

            mock_rpc.start.assert_called_once()

    def test_main_signal_handler_calls_stop(self):
        """main() signal handler calls rpc_server.stop()."""
        with (
            patch("sys.argv", ["maru-server"]),
            patch("maru_server.server.setup_logging"),
            patch("maru_server.server.MaruServer"),
            patch("maru_server.rpc_server.RpcServer") as mock_rpc_server_cls,
            patch("signal.signal") as mock_signal,
        ):
            mock_rpc = MagicMock()
            mock_rpc_server_cls.return_value = mock_rpc

            main()

            # signal.signal was called for SIGINT and SIGTERM
            assert mock_signal.call_count == 2

            # Extract the signal handler from SIGINT call
            sigint_calls = [
                call
                for call in mock_signal.call_args_list
                if call[0][0] == signal.SIGINT
            ]
            assert len(sigint_calls) == 1

            handler = sigint_calls[0][0][1]  # Second argument is the handler

            # Call the handler
            handler(None, None)

            # Verify stop was called
            mock_rpc.stop.assert_called_once()

    def test_main_signal_handler_sigterm(self):
        """main() SIGTERM signal handler also calls rpc_server.stop()."""
        with (
            patch("sys.argv", ["maru-server"]),
            patch("maru_server.server.setup_logging"),
            patch("maru_server.server.MaruServer"),
            patch("maru_server.rpc_server.RpcServer") as mock_rpc_server_cls,
            patch("signal.signal") as mock_signal,
        ):
            mock_rpc = MagicMock()
            mock_rpc_server_cls.return_value = mock_rpc

            main()

            # Extract the signal handler from SIGTERM call
            sigterm_calls = [
                call
                for call in mock_signal.call_args_list
                if call[0][0] == signal.SIGTERM
            ]
            assert len(sigterm_calls) == 1

            handler = sigterm_calls[0][0][1]

            # Call the handler
            handler(None, None)

            # Verify stop was called
            mock_rpc.stop.assert_called_once()

    def test_main_registers_correct_signals(self):
        """main() registers handlers for SIGINT and SIGTERM."""
        with (
            patch("sys.argv", ["maru-server"]),
            patch("maru_server.server.setup_logging"),
            patch("maru_server.server.MaruServer"),
            patch("maru_server.rpc_server.RpcServer"),
            patch("signal.signal") as mock_signal,
        ):
            main()

            # Check that signal.signal was called with correct signals
            signal_numbers = [call[0][0] for call in mock_signal.call_args_list]
            assert signal.SIGINT in signal_numbers
            assert signal.SIGTERM in signal_numbers
            assert len(signal_numbers) == 2

    def test_main_port_arg_type(self):
        """main() correctly parses port as integer."""
        with (
            patch("sys.argv", ["maru-server", "--port", "8080"]),
            patch("maru_server.server.setup_logging"),
            patch("maru_server.server.MaruServer") as mock_server_cls,
            patch("maru_server.rpc_server.RpcServer") as mock_rpc_server_cls,
            patch("signal.signal"),
        ):
            mock_server_instance = MagicMock()
            mock_server_cls.return_value = mock_server_instance
            mock_rpc = MagicMock()
            mock_rpc_server_cls.return_value = mock_rpc

            main()

            # Verify port is passed as integer
            call_kwargs = mock_rpc_server_cls.call_args[1]
            assert call_kwargs["port"] == 8080
            assert isinstance(call_kwargs["port"], int)

    def test_main_logging_called_before_server_creation(self):
        """main() calls setup_logging before creating MaruServer."""
        call_order = []

        def track_setup_logging(level):
            call_order.append("setup_logging")

        # Create a proper mock that doesn't require real initialization
        with (
            patch("sys.argv", ["maru-server"]),
            patch("maru_server.server.setup_logging", side_effect=track_setup_logging),
            patch("maru_server.server.MaruServer") as mock_server_cls,
            patch("maru_server.rpc_server.RpcServer"),
            patch("signal.signal"),
        ):

            def track_server_creation(*args, **kwargs):
                call_order.append("MaruServer.__init__")
                return MagicMock()

            mock_server_cls.side_effect = track_server_creation

            main()

            # Verify order
            assert call_order == ["setup_logging", "MaruServer.__init__"]

    def test_main_help_argument(self):
        """main() --help shows help message and exits."""
        with patch("sys.argv", ["maru-server", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            # argparse exits with code 0 for --help
            assert exc_info.value.code == 0

    def test_main_invalid_log_level(self):
        """main() with invalid log-level shows error and exits."""
        with patch("sys.argv", ["maru-server", "--log-level", "INVALID"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            # argparse exits with code 2 for invalid choices
            assert exc_info.value.code == 2
