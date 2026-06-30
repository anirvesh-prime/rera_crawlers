from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import dashboard


class DashboardDockerControlsTests(unittest.TestCase):
    def test_list_running_crawlers_reads_labeled_containers(self):
        inspect_payload = [{
            "Id": "abcdef1234567890",
            "Name": "/rera-crawler-daily",
            "State": {
                "Running": True,
                "StartedAt": "2026-07-01T00:00:00.000000000Z",
            },
            "Config": {
                "Cmd": ["--mode", "daily_light"],
                "Labels": {
                    "com.primenumbers.rera.role": "crawler",
                    "com.primenumbers.rera.mode": "daily_light",
                    "com.primenumbers.rera.sites": "kerala_rera",
                    "com.primenumbers.rera.tester": "false",
                    "com.primenumbers.rera.cmd": "--mode daily_light --site kerala_rera",
                },
            },
        }]

        with mock.patch("dashboard.subprocess.check_output") as check_output:
            check_output.side_effect = [
                "abcdef123456\n",
                json.dumps(inspect_payload),
            ]
            rows = dashboard._list_running_crawlers()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["container"], "abcdef123456")
        self.assertEqual(rows[0]["name"], "rera-crawler-daily")
        self.assertEqual(rows[0]["cmd"], "--mode daily_light --site kerala_rera")
        self.assertEqual(rows[0]["sites"], "kerala_rera")

    def test_docker_logs_since_offset_slices_bytes(self):
        with mock.patch("dashboard.subprocess.check_output", return_value=b"hello world"):
            chunk, offset = dashboard._docker_logs_since_offset("abcdef123456", 6)

        self.assertEqual(chunk, "world")
        self.assertEqual(offset, 11)

    def test_stop_container_uses_docker_stop_by_default(self):
        result = mock.Mock(returncode=0, stdout="abcdef123456\n", stderr="")
        with mock.patch("dashboard.subprocess.run", return_value=result) as run:
            dashboard._stop_container("abcdef123456")

        run.assert_called_once()
        self.assertEqual(run.call_args.args[0], ["docker", "stop", "--time", "20", "abcdef123456"])

    def test_stop_container_uses_docker_kill_when_forced(self):
        result = mock.Mock(returncode=0, stdout="abcdef123456\n", stderr="")
        with mock.patch("dashboard.subprocess.run", return_value=result) as run:
            dashboard._stop_container("abcdef123456", force=True)

        self.assertEqual(run.call_args.args[0], ["docker", "kill", "abcdef123456"])

    def test_fetch_direct_probe_data_reads_local_state_and_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "dashboard" / "sites"
            state_dir.mkdir(parents=True)
            (state_dir / "kerala_rera.json").write_text(json.dumps({
                "site_id": "kerala_rera",
                "run_id": 42,
                "run_type": "daily_light",
                "status": "completed",
                "started_at": "2026-07-01T00:00:00+00:00",
                "finished_at": "2026-07-01T00:01:00+00:00",
                "projects_found": 1,
                "projects_new": 1,
                "projects_updated": 0,
                "projects_skipped": 0,
                "documents_uploaded": 0,
                "error_count": 0,
                "sentinel_passed": True,
                "elapsed_s": 60.0,
            }), encoding="utf-8")
            log_dir = root / "kerala_rera"
            log_dir.mkdir()
            (log_dir / "2026-07-01_000000.jsonl").write_text(
                json.dumps({
                    "timestamp": "2026-07-01T00:00:10+00:00",
                    "site_id": "kerala_rera",
                    "run_id": 42,
                    "level": "INFO",
                    "message": "Sentinel coverage: 4/5 fields",
                    "step": "sentinel",
                    "extra": {"covered": 4, "expected": 5},
                }) + "\n" +
                json.dumps({
                    "timestamp": "2026-07-01T00:00:20+00:00",
                    "site_id": "kerala_rera",
                    "run_id": 42,
                    "level": "INFO",
                    "message": "Timing [search]: 3.00s",
                    "step": "timing",
                    "extra": {"phase": "search", "elapsed_s": 3.0},
                }) + "\n",
                encoding="utf-8",
            )

            with mock.patch.object(dashboard, "_LOGS_DIR", root), mock.patch(
                "dashboard.orchestrator_state_path",
                return_value=root / "dashboard" / "orchestrator.json",
            ):
                data = dashboard._fetch_direct_probe_data()

        self.assertEqual(data["source"], "direct-probe")
        self.assertEqual(data["latest_runs"]["kerala_rera"]["projects_found"], 1)
        self.assertTrue(data["sentinel_data"]["kerala_rera"]["passed"])
        self.assertEqual(data["sentinel_data"]["kerala_rera"]["covered"], 4)
        self.assertEqual(data["timing_by_site"]["kerala_rera"]["search"], 3.0)


if __name__ == "__main__":
    unittest.main()
