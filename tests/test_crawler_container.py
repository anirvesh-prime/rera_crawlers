from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.crawler_container import (
    assert_no_duplicate_normal_run,
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
        self.assertIn("CHROME_BIN=/usr/bin/chromium", cmd)
        self.assertIn("CHROMEDRIVER_BIN=/usr/bin/chromedriver", cmd)
        self.assertIn("--pids-limit", cmd)
        self.assertIn("--tmpfs", cmd)
        self.assertIn("--label", cmd)
        self.assertIn("com.primenumbers.rera.role=crawler", cmd)
        self.assertIn("com.primenumbers.rera.mode=daily_light", cmd)
        self.assertIn("com.primenumbers.rera.sites=kerala_rera", cmd)
        self.assertIn("-v", cmd)
        self.assertIn("test-image", cmd)
        self.assertEqual(cmd[-6:], ["--mode", "daily_light", "--site", "kerala_rera", "--item-limit", "2"])

    def test_duplicate_normal_run_is_rejected_unless_allowed(self):
        with mock.patch(
            "scripts.crawler_container._running_normal_crawler_containers",
            return_value=["abc123 rera-crawler-daily"],
        ):
            with self.assertRaisesRegex(RuntimeError, "already running"):
                assert_no_duplicate_normal_run(["--mode", "daily_light"])
            assert_no_duplicate_normal_run(["--mode", "daily_light"], allow_concurrent=True)
            assert_no_duplicate_normal_run(["--tester", "--site", "kerala_rera"])

    def test_start_detached_does_not_duplicate_remove_argument(self):
        result = mock.Mock(returncode=0, stdout="abcdef1234567890\n", stderr="")
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("scripts.crawler_container._running_normal_crawler_containers", return_value=[]), \
             mock.patch("scripts.crawler_container.subprocess.run", return_value=result) as run:
            info = __import__("scripts.crawler_container", fromlist=["start_detached"]).start_detached(
                ["--mode", "daily_light", "--site", "odisha_rera"],
                image="test-image",
                env_file=Path(tmp) / "missing.env",
                logs_dir=Path(tmp) / "logs",
                remove=None,
            )

        self.assertEqual(info["container_id"], "abcdef1234567890")
        cmd = run.call_args.args[0]
        self.assertIn("--detach", cmd)
        self.assertNotIn("--rm", cmd)

    def test_default_container_name_is_stable_shape(self):
        name = default_container_name(["--mode", "weekly_deep", "--site", "goa_rera"])
        self.assertRegex(name, r"^rera-crawler-weekly_deep-goa_rera-\d{8}-\d{6}-\d+$")


if __name__ == "__main__":
    unittest.main()
