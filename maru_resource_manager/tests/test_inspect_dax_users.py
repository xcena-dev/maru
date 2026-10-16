import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "tools/inspect_dax_users.py"
spec = importlib.util.spec_from_file_location("inspect_dax", SCRIPT)
scanner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scanner)


class InspectDaxUsersTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.proc = Path(self.tmp.name) / "proc"
        self.process = self.proc / "123"
        (self.process / "task" / "123").mkdir(parents=True)
        (self.process / "fd").mkdir()
        (self.process / "map_files").mkdir()
        (self.process / "comm").write_text("example-worker\n")
        (self.process / "maps").write_text(
            "1000-2000 rw-s 00200000 00:01 123 /dev/renamed-device\n"
        )

    def scan(self):
        # Use existing harmless character-device inodes as stand-ins. Neither
        # the test nor scanner opens them; all proc entries are temporary files.
        return scanner.inspect(["/dev/null"], self.proc)

    def test_mapping_is_found_without_an_open_descriptor(self):
        mapping = self.process / "map_files" / "1000-2000"
        mapping.symlink_to("/dev/null")
        report = self.scan()
        self.assertEqual(scanner.report_status(report), ("IN_USE", 1))
        self.assertEqual(len(report["references"]), 1)
        ref = report["references"][0]
        self.assertEqual(ref["kind"], "map_files")
        self.assertEqual(ref["name"], "example-worker")
        self.assertIn("00200000", ref["maps"])

    def test_descriptor_alias_matches_device_identity(self):
        alias = Path(self.tmp.name) / "different-device-name"
        alias.symlink_to("/dev/null")
        (self.process / "fd" / "7").symlink_to(alias)
        (self.process / "fd" / "8").symlink_to("/dev/zero")
        report = self.scan()
        self.assertEqual(scanner.report_status(report), ("IN_USE", 1))
        self.assertEqual(len(report["references"]), 1)
        self.assertTrue(report["references"][0]["reference"].endswith("/fd/7"))

    def test_permission_failure_cannot_report_no_references(self):
        original = Path.iterdir

        def restricted(path):
            if path == self.process / "map_files":
                raise PermissionError("inspection denied")
            return original(path)

        with patch.object(Path, "iterdir", restricted):
            report = self.scan()
        self.assertEqual(scanner.report_status(report), ("INCOMPLETE", 2))
        self.assertIn("inspection denied", report["errors"][0])

    def test_missing_map_files_for_live_task_is_incomplete(self):
        (self.process / "map_files").rmdir()
        self.assertEqual(scanner.report_status(self.scan()), ("INCOMPLETE", 2))

    def test_empty_successful_scan_has_no_references(self):
        self.assertEqual(
            scanner.report_status(self.scan()), ("NO_REFERENCES_OBSERVED", 0)
        )


if __name__ == "__main__":
    unittest.main()
