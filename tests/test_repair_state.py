from __future__ import annotations

from core.config import settings
from core.repair_state import create_repair_attempt, list_repair_attempts, reset_repair_attempt, update_repair_attempt


def test_repair_attempt_is_limited_to_one_until_reset(tmp_path):
    original_log_dir = settings.LOG_DIR
    settings.LOG_DIR = str(tmp_path)
    try:
        assert create_repair_attempt(
            site_id="kerala_rera",
            run_id=123,
            status="running",
            reason="failed",
            codex_command="codex exec <PROMPT>",
            prompt="prompt",
            tester_output="tester",
        )
        assert not create_repair_attempt(
            site_id="kerala_rera",
            run_id=124,
            status="running",
            reason="failed again",
            codex_command="codex exec <PROMPT>",
            prompt="prompt 2",
            tester_output="tester 2",
        )

        attempts = list_repair_attempts()
        assert attempts["kerala_rera"]["run_id"] == 123
        assert attempts["kerala_rera"]["reason"] == "failed"

        update_repair_attempt("kerala_rera", "completed", codex_output="done")
        attempts = list_repair_attempts()
        assert attempts["kerala_rera"]["status"] == "completed"
        assert attempts["kerala_rera"]["codex_output"] == "done"

        assert reset_repair_attempt("kerala_rera")
        assert "kerala_rera" not in list_repair_attempts()

        assert create_repair_attempt(
            site_id="kerala_rera",
            run_id=125,
            status="running",
            reason="failed after reset",
            codex_command="codex exec <PROMPT>",
            prompt="prompt 3",
            tester_output="tester 3",
        )
    finally:
        settings.LOG_DIR = original_log_dir
