"""Production RM process/TCP recovery tests with linker-isolated fake devices.

Run through CTest, or: python process_recovery_test.py /path/to/test_daemon -v
The daemon must be the CMake test-only target, never an installed/live RM.
"""

import errno
import importlib.util
import json
import os
import shutil
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

spec = importlib.util.spec_from_file_location(
    "repair", Path(__file__).resolve().parents[1] / "tools/prepare_empty_pool_repair.py"
)
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)


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

    def check_stats(self, used=None):
        used = used or {}
        response = self.request(5)
        count = struct.unpack_from("<I", response)[0]
        offset = 4
        pools = {}
        for _ in range(count):
            length, kind, total, free, alignment = struct.unpack_from(
                "<IIQQQ", response, offset
            )
            offset += 32
            path = response[offset : offset + length].decode()
            offset += length
            self.assertEqual((kind, total, alignment), (0, SIZE - ALIGN, ALIGN))
            self.assertEqual(free, total - used.get(path, 0) * ALIGN)
            pools[path] = free
        self.assertEqual(offset, len(response))
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

    def test_sigkill_replays_live_and_freed_allocations(self):
        self.start()
        live = self.allocate()
        freed = self.allocate()
        self.free(freed)
        identity = self.identity()
        wal = (self.state / WAL_NAME).read_bytes()
        self.assertTrue(wal)
        self.assertFalse((self.state / "global.meta").exists())
        self.stop(signal.SIGKILL)
        self.assertEqual((self.state / WAL_NAME).read_bytes(), wal)
        self.start()
        self.assertEqual(self.identity(), identity)
        self.access(live)
        self.free(freed, -errno.ENOENT)
        self.check_stats({"/dev/dax0.0": 1})
        next_handle = self.allocate()
        self.assertGreater(next_handle[0], freed[0])
        self.assertEqual(next_handle[1], freed[1])
        self.free(live)
        self.free(next_handle)
        self.stop()
        self.checkpoint(0, next_handle[0] + 1)
        self.start()
        self.check_stats()
        self.assertEqual(self.identity(), identity)
        self.stop()

    def test_missing_secret_rejects_live_recovery(self):
        self.start()
        self.allocate()
        self.stop(signal.SIGKILL)
        (self.state / "secret").unlink()
        wal = (self.state / WAL_NAME).read_bytes()
        self.start(expected_failure=True)
        self.assertIn("loadPools failed", self.log.read_text())
        self.assertEqual((self.state / WAL_NAME).read_bytes(), wal)
        self.assertFalse((self.state / "secret").exists())
        self.assertFalse((self.state / "global.meta").exists())

    def test_sigkill_replays_checkpoint_and_wal(self):
        self.start()
        old = self.allocate()
        identity = self.identity()
        self.stop()
        self.checkpoint(1, old[0] + 1)
        checkpoint = (self.state / "global.meta").read_bytes()
        self.start()
        live = self.allocate()
        self.free(old)
        self.stop(signal.SIGKILL)
        self.assertEqual((self.state / "global.meta").read_bytes(), checkpoint)
        self.assertTrue((self.state / WAL_NAME).read_bytes())
        self.start()
        self.access(live)
        self.free(old, -errno.ENOENT)
        self.check_stats({"/dev/dax0.0": 1})
        reused = self.allocate()
        self.assertEqual(reused[1], old[1])
        self.assertGreater(reused[0], live[0])
        self.free(live)
        self.free(reused)
        self.stop()
        self.checkpoint(0, reused[0] + 1)
        self.assertEqual(self.identity(), identity)

    def test_prepared_legacy_checkpoint_round_trip(self):
        next_id = 335380
        self.add_device(1)
        self.add_device(2)
        headers = {}
        for pool in range(3):
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
        (self.state / "global.meta").write_bytes(
            META_HEADER.pack(0x474C4F42, 2, 0, 0, next_id, 0)
        )
        (self.state / "secret").write_bytes(os.urandom(32))
        before = {p.name: p.read_bytes() for p in self.state.iterdir()}
        self.start(expected_failure=True)
        self.assertIn("offline repair required", self.log.read_text())

        backup = self.root / "backup"
        shutil.copytree(self.state, backup)
        output = self.root / "prepared"
        manifest = repair.prepare(
            backup, output, [f"/dev/dax{i}.0" for i in range(3)], self.sysfs
        )
        self.assertEqual(json.loads((output / "manifest.json").read_text()), manifest)
        self.assertEqual(manifest["next_region_id"], next_id)
        for name, data in before.items():
            self.assertEqual((backup / name).read_bytes(), data)
            self.assertEqual((self.state / name).read_bytes(), data)
        for pool in range(3):
            shutil.copyfile(
                output / f"pool_{pool}.meta", self.state / f"pool_{pool}.meta"
            )

        self.start()
        self.check_stats()
        handles = [self.allocate(pool) for pool in range(3)]
        for pool, handle in enumerate(handles):
            self.assertEqual(handle[:3], (next_id + pool, ALIGN, ALIGN))
        self.stop()
        self.checkpoint(3, next_id + 3)
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
        self.assertEqual((self.state / "secret").read_bytes(), before["secret"])

    def checkpoint_failure_round_trip(self, filename, operation, error):
        self.start()
        old = self.allocate()
        self.stop()
        self.checkpoint(1, old[0] + 1)
        before = (self.state / filename).read_bytes()
        identity = self.identity()

        # For write failures, successfully write the header and part of the
        # payload before returning ENOSPC. fsync failures occur after all bytes.
        fault = f"{filename}.tmp {operation} {error} 40"
        self.start(checkpoint_fault=fault)
        self.free(old)
        live = [self.allocate(), self.allocate(), self.allocate(1)]
        wal = (self.state / WAL_NAME).read_bytes()
        self.stop(expected_status=1)
        self.assertIn(f"Injected {operation} failure", self.log.read_text())
        self.assertIn(
            f"shutdown checkpoint not written (rc={-error}); WAL retained",
            self.log.read_text(),
        )
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

    def test_shutdown_checkpoint_write_failure_retains_wal(self):
        self.add_device(1)
        for filename in ("pool_0.meta", "pool_1.meta", "global.meta"):
            with self.subTest(file=filename):
                self.checkpoint_failure_round_trip(filename, "write", errno.ENOSPC)

    def test_shutdown_checkpoint_fsync_failure_retains_wal(self):
        self.add_device(1)
        for filename in ("pool_0.meta", "pool_1.meta", "global.meta"):
            with self.subTest(file=filename):
                self.checkpoint_failure_round_trip(filename, "fsync", errno.EIO)


if __name__ == "__main__":
    DAEMON = Path(sys.argv.pop(1)).resolve(strict=True)
    if DAEMON.name != "maru_rm_process_test_daemon":
        raise SystemExit("Pass the CMake test-only daemon, not a production RM")
    unittest.main()
