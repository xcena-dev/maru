import importlib.util
import json
import tempfile
import unittest
import zlib
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "tools/prepare_empty_pool_repair.py"
spec = importlib.util.spec_from_file_location("repair", SCRIPT)
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)


class PrepareRepairTest(unittest.TestCase):
    SIZE = 16 << 20
    ALIGN = 2 << 20

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.output = self.root / "prepared"
        self.sysfs = self.root / "sysfs"
        device = self.sysfs / "dax2.0"
        device.mkdir(parents=True)
        (device / "size").write_text(str(self.SIZE))
        (device / "align").write_text(str(self.ALIGN))
        (self.state / "global.meta").write_bytes(
            repair.GLOBAL_HEADER.pack(0x474C4F42, 2, 0, 0, 335373, 0)
        )
        payload = repair.EXTENT.pack(0, self.SIZE)
        self.original = (
            repair.POOL_HEADER.pack(
                0x4D455441, 6, 2, zlib.crc32(payload), self.SIZE - self.ALIGN, 1
            )
            + payload
        )
        (self.state / "pool_2.meta").write_bytes(self.original)

    def prepare(self):
        return repair.prepare(self.state, self.output, ["/dev/dax2.0"], self.sysfs)

    def test_prepares_valid_copy_and_preserves_source_and_next_id(self):
        source_global = (self.state / "global.meta").read_bytes()
        manifest = self.prepare()
        corrected = (self.output / "pool_2.meta").read_bytes()
        self.assertEqual(
            repair.EXTENT.unpack(corrected[32:]), (self.ALIGN, self.SIZE - self.ALIGN)
        )
        self.assertEqual(
            repair.POOL_HEADER.unpack_from(corrected)[3], zlib.crc32(corrected[32:])
        )
        self.assertEqual((self.state / "pool_2.meta").read_bytes(), self.original)
        self.assertEqual((self.state / "global.meta").read_bytes(), source_global)
        self.assertEqual(manifest["next_region_id"], 335373)
        self.assertFalse(manifest["applied"])
        self.assertFalse((self.output / "global.meta").exists())
        self.assertEqual(
            json.loads((self.output / "manifest.json").read_text()), manifest
        )

    def test_nonempty_wal_is_rejected_without_outputs(self):
        for name in ("maru-resource-manager.wal", "maru_resourced.wal"):
            with self.subTest(name=name):
                (self.state / name).write_bytes(b"unreplayed history")
                with self.assertRaises(ValueError):
                    self.prepare()
                self.assertFalse(self.output.exists())
                (self.state / name).unlink()

    def test_live_allocations_are_rejected(self):
        (self.state / "global.meta").write_bytes(
            repair.GLOBAL_HEADER.pack(0x474C4F42, 2, 0, 0, 335373, 1)
        )
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_bad_crc_and_nonlegacy_extent_are_rejected(self):
        damaged = bytearray(self.original)
        damaged[12] ^= 1
        with self.assertRaises(ValueError):
            repair.corrected_pool(bytes(damaged), 2, self.SIZE, self.ALIGN)
        payload = repair.EXTENT.pack(self.ALIGN * 2, self.SIZE - self.ALIGN * 2)
        other = (
            repair.POOL_HEADER.pack(
                0x4D455441, 6, 2, zlib.crc32(payload), self.SIZE - self.ALIGN, 1
            )
            + payload
        )
        with self.assertRaises(ValueError):
            repair.corrected_pool(other, 2, self.SIZE, self.ALIGN)

    def test_output_cannot_be_inside_source(self):
        self.output = self.state / "prepared"
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_correct_pool_is_unchanged(self):
        corrected = repair.corrected_pool(self.original, 2, self.SIZE, self.ALIGN)
        self.assertEqual(
            repair.corrected_pool(corrected, 2, self.SIZE, self.ALIGN), corrected
        )


if __name__ == "__main__":
    unittest.main()
