from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.crawler_container import (
    build_docker_run_command,
    default_container_name,
    infer_mode,
    infer_sites,
    is_tester,
)


class CrawlerContainerTests(unittest.TestCase):
    def test_infers_mode_sites_and_tester_flag(self):
        args = [
            "--tester",
            "--site", "kerala_rera,bihar_rera",
            "--site=rajasthan_rera",
            "--mode", "daily_light",
        ]
        self.assertEqual(infer_mode(args), "daily_light")
        self.assertEqual(infer_sites(args), "kerala_rera,bihar_rera,rajasthan_rera")
        self.assertTrue(is_tester(args))

    def test_build_docker_run_command_labels_and_passes_args(self):
        with tempfile.TemporaryDirectory() as tmp:
            logs_dir = Path(tmp) / "logs"
            cmd = build_docker_run_command(
                ["--mode", "daily_light", "--site", "kerala_rera", "--item-limit", "2"],
                image="test-image",
                name="fixed-name",
                env_file=Path(tmp) / "missing.env",
                logs_dir=logs_dir,
            )

        self.assertIn("--rm", cmd)
        self.assertIn("--init", cmd)
        self.assertIn("--network", cmd)
        self.assertIn("host", cmd)
        self.assertIn("--label", cmd)
        self.assertIn("com.primenumbers.rera.role=crawler", cmd)
        self.assertIn("com.primenumbers.rera.mode=daily_light", cmd)
        self.assertIn("com.primenumbers.rera.sites=kerala_rera", cmd)
        self.assertIn("-v", cmd)
        self.assertIn("test-image", cmd)
        self.assertEqual(cmd[-6:], ["--mode", "daily_light", "--site", "kerala_rera", "--item-limit", "2"])

    def test_default_container_name_is_stable_shape(self):
        name = default_container_name(["--mode", "weekly_deep", "--site", "goa_rera"])
        self.assertRegex(name, r"^rera-crawler-weekly_deep-goa_rera-\d{8}-\d{6}-\d+$")


if __name__ == "__main__":
    unittest.main()
