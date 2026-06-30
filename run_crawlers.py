#!/usr/bin/env python3
"""
RERA Crawlers Orchestrator — runs all enabled states in parallel.

Usage:
    python run_crawlers.py                      # runs all states in parallel
    python run_crawlers.py --site kerala_rera   # runs one state
    python run_crawlers.py --site kerala_rera --site bihar_rera
    python run_crawlers.py --site kerala_rera,bihar_rera
    python run_crawlers.py --item-limit 5       # cap each crawler to 5 projects
    python run_crawlers.py --no-item-limit      # override env and run unlimited
    python run_crawlers.py --mode weekly_deep   # explicit mode (default: weekly_deep)
    python run_crawlers.py --sequential         # disable parallel execution
    python run_crawlers.py --test               # skip S3 uploads and DB writes (dry run)
    python run_crawlers.py --skip-documents     # skip document downloads/uploads
    python run_crawlers.py --test-logs          # like --test but still write log tables (visible on dashboard)
    python run_crawlers.py --site karnataka_rera --target-reg-no "PRM/KA/RERA/1251/446/PR/181122/005482"
                                                # crawl only the matching project (skips sentinel)
"""
from __future__ import annotations

import json
import os
import sys
if os.environ.get("PYTHONHASHSEED") != "0":
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

import argparse
import importlib
import socket
import subprocess
import time
import traceback as tb_module
import shlex
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from core.config import settings
from core.crawler_base import close_shared_http_clients
from core.db import insert_crawl_error, insert_crawl_run, update_crawl_run
from core.logger import CrawlerLogger
from core.repair_state import create_repair_attempt, update_repair_attempt
from sites_config import select_sites

_SEP = "=" * 64


def _truncate_text(value: str, limit: int = 24000) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + f"\n\n[truncated: {len(value) - limit} chars omitted]"


def _format_codex_repair_prompt(
    *,
    site_cfg: dict,
    mode: str,
    run_id: int,
    status: str,
    counts: dict,
    reason: str,
    traceback: str,
    tester_output: str,
) -> str:
    site_id = site_cfg["id"]
    module_path = site_cfg.get("crawler_module", "")
    return f"""You are repairing one crawler in this repository.

Crawler: {site_id}
Display name: {site_cfg.get("name", site_id)}
Module: {module_path}
Mode that failed: {mode}
Run id: {run_id}
Final status: {status}
Failure reason: {reason}
Counts: {json.dumps(counts, sort_keys=True)}

Hard requirements:
- Repair only the failing crawler and shared code that is directly necessary for this failure.
- Do not run broad rewrites or unrelated formatting.
- Preserve existing crawler output schema and project key behavior.
- Add or update focused tests when practical.
- Verify with a targeted tester command before finishing:
  ./venv/bin/python run_crawlers.py --tester --site {site_id} --mode {mode} --item-limit {settings.CRAWLER_AUTO_REPAIR_TEST_ITEM_LIMIT}
- If the tester output indicates the portal is temporarily down or blocked rather than code-broken, document that and keep edits minimal.
- This is the only automatic repair attempt for {site_id}. If you cannot finish confidently, leave a concise explanation in your final response.

Original traceback or failure context:
```text
{_truncate_text(traceback or "No traceback captured.", 12000)}
```

Tester output captured after the failed run:
```text
{_truncate_text(tester_output or "No tester output captured.", 24000)}
```
"""


def _capture_tester_output(site_cfg: dict, mode: str) -> str:
    cmd = [
        sys.executable,
        "-u",
        "run_crawlers.py",
        "--tester",
        "--site",
        site_cfg["id"],
        "--mode",
        mode,
        "--item-limit",
        str(settings.CRAWLER_AUTO_REPAIR_TEST_ITEM_LIMIT),
    ]
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    env["CRAWLER_TESTER"] = "true"
    try:
        result = subprocess.run(
            cmd,
            cwd=str(Path(__file__).resolve().parent),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=settings.CRAWLER_AUTO_REPAIR_TEST_TIMEOUT_S,
        )
        return (
            "$ " + " ".join(cmd) + "\n"
            f"[exit_code={result.returncode}]\n"
            + (result.stdout or "")
        )
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        return (
            "$ " + " ".join(cmd) + "\n"
            f"[timeout_after={settings.CRAWLER_AUTO_REPAIR_TEST_TIMEOUT_S}s]\n"
            + output
        )
    except Exception as exc:
        return "$ " + " ".join(cmd) + f"\n[tester_capture_failed] {exc}"


def _maybe_auto_repair_crawler(
    *,
    site_cfg: dict,
    mode: str,
    run_id: int,
    status: str,
    counts: dict,
    reason: str,
    traceback: str = "",
    logger: CrawlerLogger | None = None,
) -> None:
    if not settings.CRAWLER_AUTO_REPAIR:
        return
    if settings.TEST_MODE or settings.CRAWLER_TESTER:
        return

    site_id = site_cfg["id"]
    tester_output = _capture_tester_output(site_cfg, mode)
    prompt = _format_codex_repair_prompt(
        site_cfg=site_cfg,
        mode=mode,
        run_id=run_id,
        status=status,
        counts=counts,
        reason=reason,
        traceback=traceback,
        tester_output=tester_output,
    )

    codex_cmd = [
        *shlex.split(settings.CRAWLER_AUTO_REPAIR_CODEX_BIN),
        "exec",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        prompt,
    ]
    codex_cmd_for_log = " ".join(shlex.quote(part) for part in codex_cmd[:-1]) + " <PROMPT>"

    inserted = create_repair_attempt(
        site_id=site_id,
        run_id=run_id,
        status="running",
        reason=reason,
        codex_command=codex_cmd_for_log,
        prompt=prompt,
        tester_output=tester_output,
    )
    if not inserted:
        if logger:
            logger.warning(
                "Auto repair skipped: crawler already used its one repair attempt; reset on dashboard to allow another",
                step="auto_repair",
                site=site_id,
            )
        return

    if logger:
        logger.warning("Auto repair started", step="auto_repair", site=site_id)

    try:
        result = subprocess.run(
            codex_cmd,
            cwd=str(Path(__file__).resolve().parent),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=settings.CRAWLER_AUTO_REPAIR_CODEX_TIMEOUT_S,
        )
        output = _truncate_text(result.stdout or "", 64000)
        final_status = "completed" if result.returncode == 0 else "failed"
        update_repair_attempt(
            site_id,
            final_status,
            codex_output=output,
            error_message=None if result.returncode == 0 else f"codex exited {result.returncode}",
        )
        if logger:
            logger.warning(
                f"Auto repair {final_status}",
                step="auto_repair",
                site=site_id,
                exit_code=result.returncode,
            )
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        update_repair_attempt(
            site_id,
            "timeout",
            codex_output=_truncate_text(output, 64000),
            error_message=f"codex timed out after {settings.CRAWLER_AUTO_REPAIR_CODEX_TIMEOUT_S}s",
        )
    except Exception as exc:
        update_repair_attempt(site_id, "failed", error_message=str(exc))


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("item limit must be greater than 0")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("delay scale must be non-negative")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RERA Crawlers Orchestrator")
    parser.add_argument(
        "--mode",
        choices=["daily_light", "weekly_deep", "full", "single", "incremental", "listing"],
        default="weekly_deep",
        help="Crawl mode (default: weekly_deep)",
    )
    parser.add_argument(
        "--site",
        "--sites",
        action="append",
        default=[],
        help=(
            "Run only the selected site ids. Repeat the flag or pass a comma-separated "
            "list, e.g. --site kerala_rera --site bihar_rera or --site kerala_rera,bihar_rera. "
            "When omitted, all enabled sites run."
        ),
    )
    item_limit_group = parser.add_mutually_exclusive_group()
    item_limit_group.add_argument(
        "--item-limit",
        type=_positive_int,
        default=None,
        help="Cap each selected crawler to this many projects for the current run.",
    )
    item_limit_group.add_argument(
        "--no-item-limit",
        action="store_true",
        default=False,
        help="Ignore CRAWL_ITEM_LIMIT from env/config and run without an item limit for this run.",
    )
    parser.add_argument(
        "--delay-scale",
        type=_non_negative_float,
        default=None,
        help=(
            "Scale per-crawler random throttling delays for this run. "
            "Use 1.0 for current behavior, 0.5 for roughly half the wait, or 0 to disable it."
        ),
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        default=False,
        help="Run sites one-by-one instead of in parallel (useful for debugging)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        default=False,
        help="Test mode: skip all S3 uploads and DB writes; everything else runs normally.",
    )
    parser.add_argument(
        "--test-logs",
        dest="test_logs",
        action="store_true",
        default=False,
        help=(
            "Implies --test (skips S3 uploads and data writes to rera_projects / "
            "rera_project_documents / checkpoints) but still writes the log tables "
            "(crawl_runs, crawl_logs, crawl_document_events, crawl_errors) so the "
            "test run is visible on the dashboard."
        ),
    )
    parser.add_argument(
        "--tester",
        action="store_true",
        default=False,
        help=(
            "Dashboard tester mode: implies --test (no S3 / no DB writes), forces "
            "sequential execution, requires exactly one --site, and routes every "
            "INFO/DEBUG record to stdout including per-field dumps of every "
            "extracted project.  Intended only for the dashboard 'Test Crawler' "
            "button — never use in cron."
        ),
    )
    parser.add_argument(
        "--target-reg-no",
        dest="target_reg_no",
        default=None,
        help=(
            "Restrict the run to one or more projects whose registration number "
            "matches this value (case-insensitive; pass a comma-separated list "
            "to target several).  Crawlers filter their listing rows down to "
            "those projects and skip the sentinel health check.  Combine with "
            "--test for a dry-run.  Supported by all state crawlers."
        ),
    )
    parser.add_argument(
        "--skip-documents",
        action="store_true",
        default=False,
        help=(
            "Skip document download/upload work for crawlers that support this "
            "runtime flag. Project listing/detail extraction and project DB "
            "upserts still run."
        ),
    )
    return parser.parse_args()


def _worker_init() -> None:
    """Initialiser run inside every worker process before it executes any task.
    Re-applies PYTHONHASHSEED=0 so that generate_project_key() stays deterministic
    even on platforms that use 'spawn' rather than 'fork' for new processes.
    """
    os.environ["PYTHONHASHSEED"] = "0"


def run_site(site_cfg: dict, mode: str) -> dict:
    site_id = site_cfg["id"]
    run_id  = insert_crawl_run(site_id, mode)
    logger  = CrawlerLogger(site_id, run_id)
    logger.info(f"Starting {mode} crawl", site=site_id, run_id=run_id)

    counts = {
        "projects_found":   0,
        "projects_new":     0,
        "projects_updated": 0,
        "projects_skipped": 0,
        "documents_uploaded": 0,
        "error_count":      0,
    }

    t0 = time.monotonic()
    try:
        module = importlib.import_module(site_cfg["crawler_module"])
        result = module.run(site_cfg, run_id, mode)
        counts.update(result)
        sentinel_passed = counts.pop("sentinel_passed", None)
        update_crawl_run(run_id, "completed", counts, sentinel_passed=sentinel_passed, notes=None)
        logger.info("Crawl completed", **counts)
        if counts.get("error_count", 0) > 0:
            _maybe_auto_repair_crawler(
                site_cfg=site_cfg,
                mode=mode,
                run_id=run_id,
                status="completed",
                counts=counts,
                reason=f"completed with {counts.get('error_count', 0)} error(s)",
                logger=logger,
            )
    except Exception as exc:
        counts["error_count"] += 1
        trace = tb_module.format_exc()
        update_crawl_run(run_id, "failed", counts)
        insert_crawl_error(run_id, site_id, "CRAWLER_EXCEPTION", str(exc),
                           raw_data={"traceback": trace})
        logger.error(f"Crawl failed: {exc}")
        _maybe_auto_repair_crawler(
            site_cfg=site_cfg,
            mode=mode,
            run_id=run_id,
            status="failed",
            counts=counts,
            reason=str(exc),
            traceback=trace,
            logger=logger,
        )
    finally:
        close_shared_http_clients()
        logger.log_run_key_summary(limit=10)
        logger.close()

    elapsed = time.monotonic() - t0
    return {"site_id": site_id, "run_id": run_id, "elapsed_s": round(elapsed, 1), **counts}


def _fmt_row(result: dict) -> str:
    return (
        f"  found={result['projects_found']:<5} "
        f"new={result['projects_new']:<5} "
        f"updated={result['projects_updated']:<5} "
        f"skipped={result['projects_skipped']:<5} "
        f"docs={result['documents_uploaded']:<5} "
        f"errors={result['error_count']:<4} "
        f"({result['elapsed_s']}s)"
    )


def apply_runtime_overrides(args: argparse.Namespace) -> int:
    """Apply CLI overrides to runtime settings and child worker environment."""
    # --test-logs implies --test but additionally enables DB log writes.
    if getattr(args, "test_logs", False):
        args.test = True

    # --tester implies --test and enables verbose console logging via
    # CRAWLER_TESTER.  No log-table writes either (test_logs stays off).
    if getattr(args, "tester", False):
        args.test = True
        os.environ["CRAWLER_TESTER"] = "true"
        settings.CRAWLER_TESTER = True
        # Trigger root-logger setup in the parent process so banner/print
        # output from this module is also visible in tester mode.
        from core.logger import _configure_tester_root_logger
        _configure_tester_root_logger()

    if getattr(args, "test", False):
        os.environ["TEST_MODE"] = "true"
        os.environ["DRY_RUN_S3"] = "true"
        settings.TEST_MODE = True
        settings.DRY_RUN_S3 = True

    if getattr(args, "test_logs", False):
        os.environ["TEST_MODE_LOG_TO_DB"] = "true"
        settings.TEST_MODE_LOG_TO_DB = True

    delay_scale = getattr(args, "delay_scale", None)
    if delay_scale is not None:
        os.environ["CRAWL_DELAY_SCALE"] = str(delay_scale)
        settings.CRAWL_DELAY_SCALE = delay_scale

    target_reg_no = getattr(args, "target_reg_no", None)
    if target_reg_no is not None:
        target_reg_no = target_reg_no.strip()
        os.environ["TARGET_REG_NO"] = target_reg_no
        settings.TARGET_REG_NO = target_reg_no

    if getattr(args, "skip_documents", False):
        os.environ["SKIP_DOCUMENTS"] = "true"
        settings.SKIP_DOCUMENTS = True

    if args.no_item_limit:
        os.environ.pop("CRAWL_ITEM_LIMIT", None)
        settings.CRAWL_ITEM_LIMIT = 0
        return settings.CRAWL_ITEM_LIMIT

    if args.item_limit is not None:
        os.environ["CRAWL_ITEM_LIMIT"] = str(args.item_limit)
        settings.CRAWL_ITEM_LIMIT = args.item_limit

    return settings.CRAWL_ITEM_LIMIT


def main() -> int:
    args = parse_args()
    Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)
    item_limit = apply_runtime_overrides(args)

    sites, unknown_sites, disabled_sites = select_sites(args.site)
    if unknown_sites:
        print(f"[ERROR] Unknown site id(s): {', '.join(unknown_sites)}")
        return 2
    if not sites:
        print("[ERROR] No sites selected to run.")
        return 2

    if getattr(args, "tester", False) and len(sites) != 1:
        print("[ERROR] --tester requires exactly one --site")
        return 2

    parallel = not args.sequential and not getattr(args, "tester", False) and len(sites) > 1

    started = datetime.now(timezone.utc)
    host = socket.gethostname()
    site_ids = [s["id"] for s in sites]

    # ── Orchestrator-level structured logger ─────────────────────────────────
    # Writes to the same DB / JSONL infrastructure as per-site crawlers.
    # site_id="orchestrator", run_id=None — allows operators to query
    # orchestrator events independently from individual crawler runs.
    orch_logger = CrawlerLogger("orchestrator")
    t_orch_start = time.monotonic()
    orch_logger.info(
        "Orchestrator started",
        step="startup",
        mode=args.mode,
        sites=site_ids,
        site_count=len(sites),
        parallel=parallel,
        item_limit=item_limit or "unlimited",
        skip_documents=settings.SKIP_DOCUMENTS,
        host=host,
    )

    print(f"\n{_SEP}")
    print(f"  RERA Crawler Orchestrator")
    print(f"  Mode      : {args.mode}")
    print(f"  Execution : {'parallel' if parallel else 'sequential'}")
    print(f"  Workers   : {len(sites)}")
    print(f"  Item Limit: {item_limit or 'unlimited'}")
    print(f"  Documents : {'skipped' if settings.SKIP_DOCUMENTS else 'enabled'}")
    print(f"  Host      : {host}")
    print(f"  States    : {', '.join(site_ids)}")
    if settings.TARGET_REG_NO:
        print(f"  Target    : {settings.TARGET_REG_NO}")
    print(f"  Started   : {started.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if getattr(args, "test", False):
        if getattr(args, "tester", False):
            print(f"  *** TESTER MODE (--tester): no DB / S3 writes; verbose console logging ***")
        elif getattr(args, "test_logs", False):
            print(f"  *** TEST MODE (--test-logs): S3 uploads + data writes SKIPPED; log tables WRITTEN ***")
        else:
            print(f"  *** TEST MODE: S3 uploads and DB writes are SKIPPED ***")
    if disabled_sites:
        print(f"  Disabled  : {', '.join(disabled_sites)} (explicitly selected)")
    print(f"{_SEP}\n")

    summary = []

    if parallel:
        # Cap concurrent processes: spawning one OS process per site with no
        # ceiling can exhaust file descriptors, RAM, and DB connections when
        # many states are selected.  MAX_PARALLEL_CRAWLERS (default 8) limits
        # the pool; ProcessPoolExecutor queues the remainder automatically.
        max_workers = min(len(sites), settings.MAX_PARALLEL_CRAWLERS)
        print(f"→ Launching {len(sites)} crawlers in parallel (max {max_workers} at a time)...\n")
        with ProcessPoolExecutor(max_workers=max_workers, initializer=_worker_init) as executor:
            futures = {
                executor.submit(run_site, site_cfg, args.mode): site_cfg["id"]
                for site_cfg in sites
            }
            for future in as_completed(futures):
                site_id = futures[future]
                try:
                    result = future.result()
                    summary.append(result)
                    print(f"✓ [{site_id}] finished")
                    print(f"{_fmt_row(result)}\n")
                    orch_logger.info(
                        f"Site completed: {site_id}",
                        step="site_done",
                        site=site_id,
                        run_id=result.get("run_id"),
                        elapsed_s=result.get("elapsed_s"),
                        projects_found=result.get("projects_found", 0),
                        projects_new=result.get("projects_new", 0),
                        projects_updated=result.get("projects_updated", 0),
                        projects_skipped=result.get("projects_skipped", 0),
                        documents_uploaded=result.get("documents_uploaded", 0),
                        error_count=result.get("error_count", 0),
                    )
                except Exception as exc:
                    # Worker process itself crashed (e.g. OOM, segfault in C
                    # extension).  Record a sentinel entry so the summary file
                    # and totals stay consistent — errors are counted.
                    crashed_result = {
                        "site_id": site_id,
                        "run_id": None,
                        "elapsed_s": 0.0,
                        "projects_found": 0,
                        "projects_new": 0,
                        "projects_updated": 0,
                        "projects_skipped": 0,
                        "documents_uploaded": 0,
                        "error_count": 1,
                        "crash_reason": str(exc),
                    }
                    summary.append(crashed_result)
                    print(f"✗ [{site_id}] worker process crashed: {exc}\n")
                    orch_logger.error(
                        f"Site worker crashed: {site_id} — {exc}",
                        step="site_crash",
                        site=site_id,
                        crash_reason=str(exc),
                    )
    else:
        for site_cfg in sites:
            print(f"→ [{site_cfg['id']}]  {site_cfg['name']}")
            result = run_site(site_cfg, args.mode)
            summary.append(result)
            print(f"{_fmt_row(result)}\n")
            orch_logger.info(
                f"Site completed: {site_cfg['id']}",
                step="site_done",
                site=site_cfg["id"],
                run_id=result.get("run_id"),
                elapsed_s=result.get("elapsed_s"),
                projects_found=result.get("projects_found", 0),
                projects_new=result.get("projects_new", 0),
                projects_updated=result.get("projects_updated", 0),
                projects_skipped=result.get("projects_skipped", 0),
                documents_uploaded=result.get("documents_uploaded", 0),
                error_count=result.get("error_count", 0),
            )

    # Totals
    totals = {k: sum(r.get(k, 0) for r in summary)
              for k in ("projects_found", "projects_new", "projects_updated",
                        "projects_skipped", "documents_uploaded", "error_count", "elapsed_s")}

    print(f"{_SEP}")
    print(f"  TOTALS  {_fmt_row({'site_id': 'all', 'run_id': 0, **totals})}")

    orch_logger.info(
        "Orchestrator completed",
        step="done",
        mode=args.mode,
        site_count=len(sites),
        **totals,
    )
    orch_logger.timing("total_run", time.monotonic() - t_orch_start, site_count=len(sites))
    orch_logger.close()

    # Write summary JSON to disk only when local logging is enabled.
    if settings.LOG_LOCAL:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
        summary_path = Path(settings.LOG_DIR) / "orchestrator" / f"{ts}_summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps({"mode": args.mode, "started": started.isoformat(),
                        "sites": summary, "totals": totals}, indent=2, default=str)
        )
        print(f"  Summary: {summary_path}")

    print(f"{_SEP}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
