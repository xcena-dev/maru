import argparse
import contextlib
import importlib.util
import io
import socket
import tempfile
import threading
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "tools/maru_rm_ctl.py"
spec = importlib.util.spec_from_file_location("maru_rm_ctl", SCRIPT)
ctl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ctl)


class FakeAdminSocket:
    """Accept one connection, record its request line, send a canned reply."""

    def __init__(self, path: Path, reply: str):
        self.path = path
        self.reply = reply
        self.request = None
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(path))
        self.server.listen(1)
        # Never hold the test run if the ctl fails before connecting.
        self.server.settimeout(5)
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        try:
            conn, _ = self.server.accept()
        except OSError:  # Timed out, or closed by close().
            return
        with conn:
            data = b""
            while b"\n" not in data:
                chunk = conn.recv(1024)
                if not chunk:
                    break
                data += chunk
            self.request = data.decode()
            conn.sendall(self.reply.encode())

    def close(self):
        self.thread.join(timeout=5)
        self.server.close()
        self.path.unlink()


class MaruRmCtlTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name)

    def run_ctl(self, reply, *argv):
        server = FakeAdminSocket(self.state / "admin.sock", reply)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = ctl.main(["--state-dir", str(self.state), *argv])
        server.close()
        return code, server.request, out.getvalue(), err.getvalue()

    def test_device_names_are_normalized_or_rejected(self):
        self.assertEqual(ctl.device_name("dax2.0"), "dax2.0")
        self.assertEqual(ctl.device_name("/dev/dax12.3"), "dax12.3")
        for bad in ("dax02.0", "dax2", "sda", "/dev/../dax2.0", "dax2.0 "):
            with self.assertRaises(argparse.ArgumentTypeError):
                ctl.device_name(bad)

    def test_requests_match_the_admin_protocol(self):
        cases = [
            (["status"], "status\n"),
            (["allocations"], "allocations\n"),
            (["allocations", "/dev/dax2.0"], "allocations dax2.0\n"),
            (["exclude", "/dev/dax2.0"], "exclude dax2.0\n"),
            (["include", "dax2.0", "--reinit"], "include dax2.0 --reinit\n"),
            (["include", "dax2.0", "--adopt"], "include dax2.0 --adopt\n"),
            (
                ["include", "dax2.0", "--restore-header"],
                "include dax2.0 --restore-header\n",
            ),
            (["discard", "/dev/dax2.0"], "discard dax2.0\n"),
            (["discard", "dax2.0", "--confirm"], "discard dax2.0 --confirm\n"),
            (
                ["discard", "dax2.0", "--confirm", "--force"],
                "discard dax2.0 --confirm --force\n",
            ),
            (["release", "42"], "release 42\n"),
            (
                ["release", "42", "--confirm", "--force"],
                "release 42 --confirm --force\n",
            ),
            (["release", "42", "--confirm"], "release 42 --confirm\n"),
            (["wipe", "/dev/dax2.0"], "wipe dax2.0\n"),
            (["wipe", "dax2.0", "--confirm"], "wipe dax2.0 --confirm\n"),
        ]
        for argv, expected in cases:
            code, request, out, _ = self.run_ctl("OK\nbody\n", *argv)
            self.assertEqual((code, request, out), (0, expected, "body\n"), argv)

    def test_refusal_is_reported_with_its_details(self):
        code, _, out, err = self.run_ctl(
            "ERR 16 Device or resource busy\nregion=7 client=h:1 owner=remote\n",
            "exclude",
            "dax2.0",
        )
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("refused: Device or resource busy", err)
        self.assertIn("region=7", err)

    def test_unfinished_exclusion_is_neither_success_nor_refusal(self):
        code, _, out, err = self.run_ctl(
            "INCOMPLETE\nexcluded /dev/dax2.0 (pool 2)\nnot durable yet\n",
            "exclude",
            "dax2.0",
        )
        self.assertEqual(code, 4)
        self.assertEqual(out, "excluded /dev/dax2.0 (pool 2)\nnot durable yet\n")
        self.assertIn("not finished", err)
        self.assertNotIn("refused", err)

    def test_unreachable_rm_is_distinct_from_refusal(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = ctl.main(["--state-dir", str(self.state), "status"])
        self.assertEqual(code, 2)
        self.assertIn("cannot reach maru-resource-manager", err.getvalue())

    def test_no_reply_is_not_reported_as_unreachable(self):
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(self.state / "admin.sock"))
        server.listen(1)  # Accepts the connection, never answers.
        self.addCleanup(server.close)
        err = io.StringIO()
        original = ctl.send
        ctl.send = lambda path, line, timeout: original(path, line, 0.2)
        self.addCleanup(setattr, ctl, "send", original)
        with contextlib.redirect_stderr(err):
            code = ctl.main(["--state-dir", str(self.state), "include", "dax2.0"])
        self.assertEqual(code, 3)
        self.assertIn("may still complete", err.getvalue())

    def test_bad_arguments_never_reach_the_rm(self):
        for argv in (["exclude", "sda"], ["release", "-1"], ["include"], ["wipe"],
                     ["discard", "dax2.0", "--force"],
                     ["release", "42", "--force"],
                     ["include", "dax2.0", "--adopt", "--reinit"]):
            with (
                self.assertRaises(SystemExit),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                ctl.main(["--state-dir", str(self.state), *argv])
        self.assertFalse((self.state / "admin.sock").exists())


if __name__ == "__main__":
    unittest.main()
