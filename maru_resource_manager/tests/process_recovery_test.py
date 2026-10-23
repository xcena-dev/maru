"""Production RM process/TCP recovery tests with linker-isolated fake devices.

Run through CTest, or: python process_recovery_test.py /path/to/test_daemon -v
The daemon must be the CMake test-only target, never an installed/live RM.
"""

import errno
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
import zlib
from pathlib import Path

DAEMON = None
ALIGN = 2 << 20
SIZE = 16 << 20
WIRE_HEADER = struct.Struct("<IHHI")
HANDLE = struct.Struct("<QQQQ")
META_HEADER = struct.Struct("<IIIIQQ")
EXTENT = struct.Struct("<QQ")
WAL_NAME = "maru-resource-manager.wal"
CTL = Path(__file__).resolve().parents[1] / "tools/maru_rm_ctl.py"


class ProcessRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="maru-rm-process-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / ".rm-test-fixture").touch()
        self.state = self.root / "state"
        self.state.mkdir()
        self.sysfs = self.root / "sys/bus/dax/devices"
        self.sysfs.mkdir(parents=True)
        (self.root / "sys/class/block").mkdir(parents=True)
        (self.root / "dev").mkdir()
        self.process = None
        self.connection = None
        self.starts = 0
        self.addCleanup(self.cleanup_process)
        client_id = f"{socket.gethostname()}:{os.getpid()}".encode()
        self.client_id = struct.pack("<H", len(client_id)) + client_id
        self.add_device(0)

    def add_device(self, pool):
        name = f"dax{pool}.0"
        device = self.root / "dev" / name
        # Ordinary sparse files; no /dev node is opened by the Python client.
        with device.open("wb") as stream:
            stream.truncate(SIZE)
        sysfs = self.sysfs / name
        sysfs.mkdir()
        (sysfs / "driver").symlink_to("/sys/bus/dax/drivers/device_dax")
        (sysfs / "size").write_text(str(SIZE))
        (sysfs / "align").write_text(str(ALIGN))
        class_path = self.root / "sys/class/dax" / name
        class_path.mkdir(parents=True)
        (class_path / "size").write_text(str(SIZE))
        return device

    def cleanup_process(self):
        if self.connection:
            self.connection.close()
            self.connection = None
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)

    def start(self, expected_failure=False, checkpoint_fault=None):
        self.assertTrue(self.process is None or self.process.poll() is not None)
        self.starts += 1
        self.log = self.root / f"daemon-{self.starts}.log"
        (self.root / "port").unlink(missing_ok=True)
        env = dict(os.environ, MARU_TEST_DAX_ROOT=str(self.root))
        env.pop("MARU_TEST_CHECKPOINT_FAILURE", None)
        if checkpoint_fault is not None:
            env["MARU_TEST_CHECKPOINT_FAILURE"] = checkpoint_fault
        with self.log.open("wb") as log:
            self.process = subprocess.Popen(
                [str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT
            )
        if expected_failure:
            self.assertEqual(self.process.wait(timeout=10), 1, self.log.read_text())
            self.assertNotIn("ready — listening", self.log.read_text())
            self.assertFalse((self.root / "port").exists(), "bound a port on failure")
            return
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.fail(f"Daemon exited: {self.log.read_text()}")
            if "ready — listening" in self.log.read_text():
                port = int((self.root / "port").read_text())
                self.connection = socket.create_connection(
                    ("127.0.0.1", port), timeout=5
                )
                return
            time.sleep(0.01)
        self.fail(f"Daemon did not become ready: {self.log.read_text()}")

    def stop(self, sig=signal.SIGTERM, expected_status=None):
        self.process.send_signal(sig)
        expected = (
            expected_status
            if expected_status is not None
            else 0
            if sig == signal.SIGTERM
            else -signal.SIGKILL
        )
        self.assertEqual(self.process.wait(timeout=10), expected, self.log.read_text())
        self.connection.close()
        self.connection = None

    def receive(self, size):
        data = bytearray()
        while len(data) < size:
            part = self.connection.recv(size - len(data))
            self.assertTrue(part, "RM closed the connection before responding")
            data.extend(part)
        return bytes(data)

    def request(self, kind, payload=b"", expected_kind=None):
        self.connection.sendall(
            WIRE_HEADER.pack(0x4D415255, 2, kind, len(payload)) + payload
        )
        magic, version, response_kind, size = WIRE_HEADER.unpack(
            self.receive(WIRE_HEADER.size)
        )
        self.assertEqual((magic, version), (0x4D415255, 2))
        self.assertLess(size, 1 << 20)
        response = self.receive(size)
        self.assertEqual(response_kind, expected_kind or kind + 1, repr(response))
        return response

    def allocate(self, pool=0):
        path = f"/dev/dax{pool}.0".encode()
        payload = struct.pack("<QII", ALIGN, len(path), 0) + path + self.client_id
        response = self.request(1, payload)
        self.assertEqual(struct.unpack_from("<i", response)[0], 0)
        handle = HANDLE.unpack_from(response, 8)
        self.assertGreaterEqual(handle[1], ALIGN)
        self.assertEqual(handle[2], ALIGN)
        path_len = struct.unpack_from("<I", response, 48)[0]
        self.assertEqual(response[52 : 52 + path_len], path)
        return handle

    def free(self, handle, status=0):
        response = self.request(3, HANDLE.pack(*handle) + self.client_id)
        self.assertEqual(struct.unpack("<i", response)[0], status)

    def access(self, handle, pool=0):
        response = self.request(9, HANDLE.pack(*handle) + self.client_id)
        status, path_len = struct.unpack_from("<iI", response)
        self.assertEqual(status, 0)
        self.assertEqual(response[8 : 8 + path_len], f"/dev/dax{pool}.0".encode())
        self.assertEqual(struct.unpack_from("<QQ", response, 8 + path_len), handle[1:3])

    def stats(self):
        """Served pools by device path: (kind, total, free, alignment)."""
        response = self.request(5)
        offset, pools = 4, {}
        for _ in range(struct.unpack_from("<I", response)[0]):
            length, *fields = struct.unpack_from("<IIQQQ", response, offset)
            offset += 32
            pools[response[offset : offset + length].decode()] = tuple(fields)
            offset += length
        self.assertEqual(offset, len(response))
        return pools

    def check_stats(self, used=None):
        used = used or {}
        pools = self.stats()
        for path, (kind, total, free, alignment) in pools.items():
            self.assertEqual((kind, total, alignment), (0, SIZE - ALIGN, ALIGN))
            self.assertEqual(free, total - used.get(path, 0) * ALIGN)
        self.assertEqual(set(pools), {f"/dev/{p.name}" for p in self.sysfs.iterdir()})

    def identity(self):
        with (self.root / "dev/dax0.0").open("rb") as stream:
            header = stream.read(ALIGN)
        self.assertEqual(header[:8], b"MARUDEV\0")
        secret = (self.state / "secret").read_bytes()
        self.assertEqual(len(secret), 32)
        return header, secret

    def checkpoint(self, expected_count, next_id=None):
        data = (self.state / "global.meta").read_bytes()
        magic, version, crc, _, saved_next, count = META_HEADER.unpack_from(data)
        self.assertEqual((magic, version, count), (0x474C4F42, 2, expected_count))
        self.assertEqual(zlib.crc32(data[META_HEADER.size :]), crc)
        self.assertFalse((self.state / WAL_NAME).exists())
        if next_id is not None:
            self.assertEqual(saved_next, next_id)

    def test_sigterm_preserves_live_handle(self):
        self.start()
        handle = self.allocate()
        identity = self.identity()
        first_pid = self.process.pid
        self.stop()
        self.checkpoint(1, handle[0] + 1)
        self.start()
        self.assertNotEqual(self.process.pid, first_pid)
        self.assertEqual(self.identity(), identity)
        self.access(handle)
        self.check_stats({"/dev/dax0.0": 1})
        next_handle = self.allocate()
        self.assertGreater(next_handle[0], handle[0])
        self.assertGreaterEqual(next_handle[1], handle[1] + handle[2])
        self.free(handle)
        self.free(next_handle)
        self.stop()
        self.checkpoint(0, next_handle[0] + 1)
        self.start()
        self.check_stats()
        self.free(handle, -errno.ENOENT)
        reused = self.allocate()
        self.assertEqual(reused[1], ALIGN)
        self.assertGreater(reused[0], next_handle[0])
        self.free(reused)
        self.stop()

    def test_missing_secret_rejects_live_recovery(self):
        self.start()
        self.allocate()
        self.stop(signal.SIGKILL)
        (self.state / "secret").unlink()
        wal = (self.state / WAL_NAME).read_bytes()
        global_meta = (self.state / "global.meta").read_bytes()
        self.start(expected_failure=True)
        self.assertIn("loadPools failed", self.log.read_text())
        self.assertEqual((self.state / WAL_NAME).read_bytes(), wal)
        self.assertFalse((self.state / "secret").exists())
        self.assertEqual((self.state / "global.meta").read_bytes(), global_meta)

    def test_sigkill_replays_checkpoint_and_wal(self):
        self.start()
        old = self.allocate()
        identity = self.identity()
        self.stop()
        self.checkpoint(1, old[0] + 1)
        checkpoint = (self.state / "global.meta").read_bytes()
        self.start()
        live = self.allocate()
        freed = self.allocate()
        self.free(freed)
        self.free(old)
        wal = (self.state / WAL_NAME).read_bytes()
        self.assertTrue(wal)
        self.stop(signal.SIGKILL)
        self.assertEqual((self.state / "global.meta").read_bytes(), checkpoint)
        self.assertEqual((self.state / WAL_NAME).read_bytes(), wal)
        self.start()
        self.assertEqual(self.identity(), identity)
        self.access(live)
        self.free(old, -errno.ENOENT)
        self.free(freed, -errno.ENOENT)
        self.check_stats({"/dev/dax0.0": 1})
        reused = self.allocate()
        self.assertEqual(reused[1], old[1])
        self.assertGreater(reused[0], freed[0])
        self.free(live)
        self.free(reused)
        self.stop()
        self.checkpoint(0, reused[0] + 1)
        self.assertEqual(self.identity(), identity)

    def test_legacy_and_resized_checkpoints_start_without_repair(self):
        next_id = 335380
        self.add_device(1)
        self.add_device(2)
        headers = {}
        for pool in range(2):
            path = self.root / f"dev/dax{pool}.0"
            header = struct.pack("<8sI16sI", b"MARUDEV\0", 1, uuid.uuid4().bytes, 0)
            with path.open("r+b") as stream:
                stream.write(header)
            headers[pool] = header
            payload = EXTENT.pack(0, SIZE)
            # Cover both historical total sizes and the observed header-inclusive extent.
            total = SIZE if pool == 0 else SIZE - ALIGN
            (self.state / f"pool_{pool}.meta").write_bytes(
                META_HEADER.pack(0x4D455441, 6, pool, zlib.crc32(payload), total, 1)
                + payload
            )
        # dax2.0 was re-provisioned to four times its size and lost its
        # header; its checkpoint still describes the old device.
        small = SIZE // 4
        payload = EXTENT.pack(ALIGN, small - ALIGN)
        (self.state / "pool_2.meta").write_bytes(
            META_HEADER.pack(0x4D455441, 6, 2, zlib.crc32(payload), small - ALIGN, 1)
            + payload
        )
        (self.state / "global.meta").write_bytes(
            META_HEADER.pack(0x474C4F42, 2, 0, 0, next_id, 0)
        )
        (self.state / "secret").write_bytes(os.urandom(32))
        secret = (self.state / "secret").read_bytes()

        self.start()
        self.check_stats()
        self.assertIn("started over", self.log.read_text())
        with (self.root / "dev/dax2.0").open("rb") as stream:
            self.assertEqual(stream.read(8), b"MARUDEV\0")
        handles = [self.allocate(pool) for pool in range(3)]
        for pool, handle in enumerate(handles):
            self.assertEqual(handle[:3], (next_id + pool, ALIGN, ALIGN))
        self.stop()
        self.checkpoint(3, next_id + 3)
        for pool in range(3):  # Recorded as version 7 before being served.
            meta = (self.state / f"pool_{pool}.meta").read_bytes()
            self.assertEqual(struct.unpack_from("<II", meta), (0x4D455441, 7))
        self.start()
        self.check_stats({f"/dev/dax{i}.0": 1 for i in range(3)})
        for pool, handle in enumerate(handles):
            self.access(handle, pool)
            self.free(handle)
        self.stop()
        self.checkpoint(0, next_id + 3)
        self.start()
        self.check_stats()
        reused = self.allocate(2)
        self.assertEqual(reused[:3], (next_id + 3, ALIGN, ALIGN))
        self.free(reused)
        self.stop()
        for pool, header in headers.items():
            with (self.root / f"dev/dax{pool}.0").open("rb") as stream:
                self.assertEqual(stream.read(len(header)), header)
        self.assertEqual((self.state / "secret").read_bytes(), secret)

    def checkpoint_failure_round_trip(self, filename, operation, error):
        self.start()
        old = self.allocate()
        self.stop()
        self.checkpoint(1, old[0] + 1)
        before = (self.state / filename).read_bytes()
        identity = self.identity()

        # A write failure writes the header and part of the payload first;
        # an fsync failure comes after all bytes.
        fault = f"{filename}.tmp {operation} {error} 40"
        self.start(checkpoint_fault=fault)
        self.free(old)
        live = [self.allocate(), self.allocate(), self.allocate(1)]
        wal = (self.state / WAL_NAME).read_bytes()
        self.stop(expected_status=1)
        self.assertIn(f"Injected {operation} failure", self.log.read_text())
        log = self.log.read_text()
        self.assertIn("WAL retained", log)
        self.assertIn(f"rc={-error}", log)
        self.assertEqual((self.state / WAL_NAME).read_bytes(), wal)
        self.assertEqual((self.state / filename).read_bytes(), before)

        # The following fresh process must replay the preserved log even when
        # earlier pool files were already replaced before a later file failed.
        self.start()
        self.assertEqual(self.identity(), identity)
        self.free(old, -errno.ENOENT)
        self.check_stats({"/dev/dax0.0": 2, "/dev/dax1.0": 1})
        for index, handle in enumerate(live):
            self.access(handle, 1 if index == 2 else 0)
        next_handle = self.allocate()
        self.assertGreater(next_handle[0], max(h[0] for h in live))
        self.assertGreaterEqual(next_handle[1], live[1][1] + live[1][2])
        for handle in [*live, next_handle]:
            self.free(handle)
        self.stop()
        self.checkpoint(0, next_handle[0] + 1)
        self.assertFalse((self.state / f"{filename}.tmp").exists())

    def test_shutdown_checkpoint_failure_retains_wal(self):
        # CheckpointFailureTest covers every file and phase in process; this
        # runs the real shutdown once, failing the last file it writes.
        self.add_device(1)
        self.checkpoint_failure_round_trip("global.meta", "write", errno.ENOSPC)

    def ctl(self, *argv):
        result = subprocess.run(
            [sys.executable, str(CTL), "--state-dir", str(self.state), *argv],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode, result.stdout + result.stderr

    def test_admin_exclude_and_include(self):
        self.add_device(1)
        self.start()
        handle = self.allocate(1)
        code, output = self.ctl("exclude", "/dev/dax1.0")
        self.assertEqual(code, 1, output)
        self.assertIn(f"region={handle[0]}", output)
        self.free(handle)
        code, output = self.ctl("wipe", "dax1.0", "--confirm")
        self.assertEqual(code, 1, output)  # Still used by the RM.
        code, output = self.ctl("exclude", "dax1.0")
        self.assertEqual(code, 0, output)
        self.assertEqual(set(self.stats()), {"/dev/dax0.0"})
        header = (self.root / "dev/dax1.0").read_bytes()[:64]

        # The decision survives a restart; the device is never touched.
        self.stop()
        self.start()
        self.assertEqual(set(self.stats()), {"/dev/dax0.0"})
        self.assertEqual((self.root / "dev/dax1.0").read_bytes()[:64], header)
        code, output = self.ctl("status")
        self.assertEqual(code, 0, output)
        self.assertIn("device=/dev/dax1.0 state=excluded", output)

        # Reset it for reuse: a wiped device comes back with a fresh header.
        code, output = self.ctl("wipe", "dax1.0")
        self.assertEqual(code, 1, output)  # Needs --confirm.
        code, output = self.ctl("wipe", "dax1.0", "--confirm")
        self.assertEqual(code, 0, output)
        with (self.root / "dev/dax1.0").open("rb") as stream:
            self.assertEqual(stream.read(4096), bytes(4096))
        code, output = self.ctl("include", "dax1.0")
        self.assertEqual(code, 0, output)
        self.assertEqual(set(self.stats()), {"/dev/dax0.0", "/dev/dax1.0"})
        with (self.root / "dev/dax1.0").open("rb") as stream:
            self.assertEqual(stream.read(8), b"MARUDEV\0")
        self.access(self.allocate(1), 1)
        self.stop()
        self.assertFalse((self.state / "admin.sock").exists())

        # dax1.0 loses its header (its memory was power-cycled) while this
        # process, a local owner, still holds an allocation on it: refused,
        # dax0.0 is served, and discard settles it.
        with (self.root / "dev/dax1.0").open("r+b") as stream:
            stream.write(bytes(32))
        self.start()
        self.assertEqual(set(self.stats()), {"/dev/dax0.0"})
        code, output = self.ctl("status")
        self.assertEqual(code, 0, output)
        self.assertIn("device=/dev/dax1.0 pool=1 state=refused", output)
        code, output = self.ctl("discard", "dax1.0", "--confirm")
        self.assertNotEqual(code, 0, output)  # The owner is running.
        code, output = self.ctl("discard", "dax1.0", "--confirm", "--force")
        self.assertEqual(code, 0, output)
        self.process.send_signal(signal.SIGHUP)  # Rescan now.
        deadline = time.monotonic() + 10
        while set(self.stats()) != {"/dev/dax0.0", "/dev/dax1.0"}:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.1)
        self.stop()

    def hold(self, handle, holder=b"node-a", instance=b"epoch-1", expected_kind=None):
        fixed = HANDLE.pack(*handle) + struct.pack("<II", len(holder), len(instance))
        return self.request(
            11, fixed + holder + instance + self.client_id, expected_kind
        )

    def unhold(self, handle, holder=b"node-a", instance=b"epoch-1", expected_kind=None):
        fixed = HANDLE.pack(*handle) + struct.pack("<II", len(holder), len(instance))
        response = self.request(
            13, fixed + holder + instance + self.client_id, expected_kind
        )
        if expected_kind:
            return response
        self.assertEqual(len(response), 4)
        return struct.unpack("<i", response)[0]

    def access_status(self, handle):
        response = self.request(9, HANDLE.pack(*handle) + self.client_id)
        return struct.unpack_from("<i", response)[0]

    def test_region_holds_over_tcp(self):
        self.start()
        handle = self.allocate()
        response = self.hold(handle)
        status, path_len = struct.unpack_from("<iI", response)
        self.assertEqual(status, 0)
        self.assertEqual(response[8 : 8 + path_len], b"/dev/dax0.0")
        self.assertEqual(struct.unpack_from("<QQ", response, 8 + path_len), handle[1:3])
        # A bad token is an error reply on a connection that stays open; a bad
        # name is the request's own status.
        bad = (*handle[:3], handle[3] ^ 1)
        response = self.hold(bad, expected_kind=255)
        self.assertEqual(struct.unpack_from("<i", response)[0], -errno.EACCES)
        for holder, instance in ((b"node a", b"epoch-1"), (b"node-a", b"epoch\x001")):
            response = self.hold(handle, holder=holder, instance=instance)
            self.assertEqual(struct.unpack_from("<iI", response), (-errno.EINVAL, 0))
            self.assertEqual(self.unhold(handle, holder, instance), -errno.EINVAL)
        response = self.unhold(bad, expected_kind=255)
        self.assertEqual(struct.unpack_from("<i", response)[0], -errno.EACCES)
        self.access(handle)  # The connection stays open.

        # Freed while held: freed for its owner, out of use for everyone.
        self.free(handle)
        self.assertEqual(self.access_status(handle), -errno.ENOENT)
        other = self.allocate()
        self.assertNotEqual(other[1], handle[1])
        self.stop()
        self.start()
        self.assertEqual(self.access_status(handle), -errno.ENOENT)
        self.check_stats({"/dev/dax0.0": 2})
        self.assertEqual(self.unhold(handle, instance=b"epoch-0"), 0)
        self.check_stats({"/dev/dax0.0": 2})
        self.assertEqual(self.unhold(handle), 0)
        self.assertEqual(self.unhold(handle), 0)  # No longer allocated.
        self.check_stats({"/dev/dax0.0": 1})
        reused = self.allocate()
        self.assertEqual(reused[1], handle[1])

        # A new agent instance drops what its predecessor held.
        self.assertEqual(struct.unpack_from("<i", self.hold(reused))[0], 0)
        self.free(reused)
        fixed = struct.pack("<II", 6, 7)
        response = self.request(15, fixed + b"node-a" + b"epoch-2" + self.client_id)
        self.assertEqual(struct.unpack("<iI", response), (0, 1))
        self.check_stats({"/dev/dax0.0": 1})
        code, output = self.ctl("status")
        self.assertEqual(code, 0, output)
        self.assertIn("holds=0 draining=0", output)

        # Truncated names close the connection; so does an unknown type, which
        # is how a client learns that an RM has no holds.
        truncated = HANDLE.pack(*other) + struct.pack("<II", 100, 1) + b"x"
        response = self.request(13, truncated, expected_kind=255)
        self.assertEqual(struct.unpack_from("<i", response)[0], -errno.EPROTO)
        self.assertEqual(self.connection.recv(1), b"")
        self.connection.close()
        port = int((self.root / "port").read_text())
        self.connection = socket.create_connection(("127.0.0.1", port), timeout=5)
        response = self.request(17, expected_kind=255)
        self.assertEqual(struct.unpack_from("<i", response)[0], -errno.ENOSYS)
        self.assertEqual(self.connection.recv(1), b"")
        self.connection.close()
        self.connection = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.free(other)
        self.stop()
        self.checkpoint(0)

    def test_second_instance_is_refused(self):
        self.start()
        handle = self.allocate()
        env = dict(os.environ, MARU_TEST_DAX_ROOT=str(self.root))
        second = subprocess.run(
            [str(DAEMON)], env=env, capture_output=True, text=True, timeout=10
        )
        self.assertEqual(second.returncode, 1, second.stdout + second.stderr)
        self.assertIn("cannot lock", second.stdout + second.stderr)
        code, output = self.ctl("status")  # The first RM keeps its admin socket.
        self.assertEqual(code, 0, output)
        self.access(handle)
        self.free(handle)
        self.stop()


if __name__ == "__main__":
    DAEMON = Path(sys.argv.pop(1)).resolve(strict=True)
    if DAEMON.name != "maru_rm_process_test_daemon":
        raise SystemExit("Pass the CMake test-only daemon, not a production RM")
    unittest.main()
