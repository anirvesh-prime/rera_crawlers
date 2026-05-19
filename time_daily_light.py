#!/usr/bin/env python3
"""
Speed-test for daily_light across all enabled states — parallel edition.

  - item_limit  : 50 projects per state
  - delay_scale : 0  → no artificial throttle
  - uploads     : patched to no-op (nothing written to S3)
  - checkpoints : patched to no-op (no pollution)
  - Each state runs in its own process (max 8 at once)
  - One tqdm bar per state updates live; results print as each finishes
  - Grand wall-clock total is the true end-to-end time

Usage:
    PYTHONHASHSEED=0 python time_daily_light.py
"""
from __future__ import annotations

import importlib
import os
import sys
import time
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── Must be set before any project-key generation ────────────────────────────
if os.environ.get("PYTHONHASHSEED") != "0":
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

# ── No delays, 50-item cap (main process) ────────────────────────────────────
os.environ["CRAWL_DELAY_SCALE"] = "0"
os.environ["CRAWL_ITEM_LIMIT"]  = "50000"

# ── Patch S3 + checkpoint before any site module is imported ─────────────────
import core.s3 as _s3  # noqa: E402
_s3.upload_document = lambda *_a, **_kw: None  # type: ignore[assignment]

from core.config import settings  # noqa: E402
settings.CRAWL_DELAY_SCALE = 0.0
settings.CRAWL_ITEM_LIMIT  = 50

from core.db import insert_crawl_run, update_crawl_run  # noqa: E402
import core.checkpoint as _ckpt  # noqa: E402
_ckpt.save_checkpoint = lambda *_a, **_kw: None  # no checkpoint pollution

# ── Mock get_project_by_key → always "found" ─────────────────────────────────
# Forces every project into the skip branch so daily_light never falls through
# to detail_fetch / doc_downloads regardless of actual DB state.
import core.db as _db  # noqa: E402
_db.get_project_by_key = lambda *_a, **_kw: {"id": 1}  # type: ignore[assignment]

from sites_config import SITES  # noqa: E402

try:
    from tqdm import tqdm
except ImportError:
    print("tqdm not found — run:  pip install tqdm")
    sys.exit(1)

_SEP = "─" * 78
_MAX_WORKERS = 8


# ── Worker process helpers ────────────────────────────────────────────────────

def _worker_init() -> None:
    """Re-apply all patches inside every spawned worker process."""
    os.environ["CRAWL_DELAY_SCALE"] = "0"
    os.environ["CRAWL_ITEM_LIMIT"]  = "50"

    import core.s3 as s3
    s3.upload_document = lambda *_a, **_kw: None  # type: ignore[assignment]

    from core.config import settings as cfg
    cfg.CRAWL_DELAY_SCALE = 0.0
    cfg.CRAWL_ITEM_LIMIT  = 50000

    import core.checkpoint as ckpt
    ckpt.save_checkpoint = lambda *_a, **_kw: None

    import core.db as db
    db.get_project_by_key = lambda *_a, **_kw: {"id": 1}  # type: ignore[assignment]

    # Silence console output — tqdm bars live in the main process only
    import logging
    logging.StreamHandler.emit = lambda self, record: None  # type: ignore[method-assign]


def _run_one(site: dict) -> dict:
    """Run one state's daily_light; return counts + elapsed_s + error."""
    from core.db import insert_crawl_run, update_crawl_run  # re-import in worker
    run_id = insert_crawl_run(site["id"], "daily_light")
    module  = importlib.import_module(site["crawler_module"])
    t0 = time.monotonic()
    try:
        result  = module.run(site, run_id, "daily_light")
        elapsed = time.monotonic() - t0
        update_crawl_run(run_id, "completed", result)
        return {**result, "elapsed_s": round(elapsed, 2), "error": None}
    except Exception as exc:
        elapsed = time.monotonic() - t0
        update_crawl_run(run_id, "failed", {})
        return {
            "projects_found": 0, "projects_new": 0, "projects_updated": 0,
            "projects_skipped": 0, "documents_uploaded": 0, "error_count": 1,
            "elapsed_s": round(elapsed, 2), "error": str(exc),
        }


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    enabled     = [s for s in SITES if s["enabled"]]
    n           = len(enabled)
    max_workers = min(n, _MAX_WORKERS)

    print(f"\n{_SEP}")
    print(f"  daily_light speed test  —  50 samples/state  delay=0  uploads=off  parallel")
    print(f"  States: {n}    Workers: {max_workers}")
    print(f"{_SEP}\n")

    # ── One tqdm bar per state ────────────────────────────────────────────────
    bars:       dict[str, tqdm] = {}
    bar_starts: dict[str, float] = {}
    completed:  set[str] = set()
    lock = threading.Lock()

    for i, site in enumerate(enabled):
        sid = site["id"]
        bars[sid] = tqdm(
            total=1,
            desc=f"  {sid:<32}",
            position=i,
            leave=True,
            bar_format="{desc} {bar:18} {postfix}",
            postfix="queued",
            dynamic_ncols=False,
        )

    # ── Background ticker: refresh elapsed on running bars every 0.4 s ───────
    ticker_stop = threading.Event()

    def _tick() -> None:
        while not ticker_stop.is_set():
            time.sleep(0.4)
            with lock:
                for sid, bar in bars.items():
                    if sid not in completed and sid in bar_starts:
                        elapsed = time.monotonic() - bar_starts[sid]
                        bar.set_postfix_str(f"⏱ {elapsed:>6.1f}s  running…")
                        bar.refresh()

    ticker = threading.Thread(target=_tick, daemon=True)
    ticker.start()

    # ── Launch all states in parallel ─────────────────────────────────────────
    results: list[tuple[str, dict]] = []
    grand_start = time.monotonic()

    with ProcessPoolExecutor(max_workers=max_workers, initializer=_worker_init) as pool:
        futures: dict = {}
        for site in enabled:
            fut = pool.submit(_run_one, site)
            futures[fut] = site
            with lock:
                bar_starts[site["id"]] = time.monotonic()
                bars[site["id"]].set_postfix_str("🚀 starting…")
                bars[site["id"]].refresh()

        for fut in as_completed(futures):
            site = futures[fut]
            sid  = site["id"]
            try:
                res = fut.result()
            except Exception as exc:
                res = {
                    "projects_found": 0, "projects_new": 0, "projects_updated": 0,
                    "projects_skipped": 0, "documents_uploaded": 0, "error_count": 1,
                    "elapsed_s": round(time.monotonic() - bar_starts.get(sid, grand_start), 2),
                    "error": str(exc),
                }

            results.append((sid, res))
            ok = not res["error"]
            tag = "✓" if ok else "✗ ERROR"

            with lock:
                completed.add(sid)
                bars[sid].set_postfix_str(
                    f"{res['elapsed_s']:>7.2f}s  "
                    f"found={res['projects_found']:<4} "
                    f"skip={res['projects_skipped']:<4} "
                    f"new={res['projects_new']:<3}  [{tag}]"
                )
                bars[sid].update(1)

            if res["error"]:
                tqdm.write(f"  ↳ ERROR {sid}: {res['error']}")

    ticker_stop.set()
    ticker.join()

    for bar in bars.values():
        bar.close()

    grand_elapsed = time.monotonic() - grand_start

    # ── Summary table (sorted slowest → fastest) ──────────────────────────────
    results.sort(key=lambda x: x[1]["elapsed_s"], reverse=True)

    print(f"\n\n{_SEP}")
    print(f"  SUMMARY  —  wall-clock total: {grand_elapsed:.2f}s")
    print(f"{_SEP}")
    print(f"  {'State':<32}  {'Time':>8}  {'Found':>6}  {'Skipped':>8}  {'New':>4}  {'Err':>4}")
    print(f"  {'─'*32}  {'─'*8}  {'─'*6}  {'─'*8}  {'─'*4}  {'─'*4}")
    for sid, res in results:
        flag = " ⚠" if res["error"] or res.get("error_count", 0) else ""
        print(
            f"  {sid:<32}  {res['elapsed_s']:>7.2f}s  "
            f"{res['projects_found']:>6}  {res['projects_skipped']:>8}  "
            f"{res['projects_new']:>4}  {res.get('error_count', 0):>4}{flag}"
        )
    print(f"  {'─'*32}  {'─'*8}")
    print(f"  {'TOTAL  (' + str(n) + ' states)':<32}  {grand_elapsed:>7.2f}s  ← wall-clock")
    print(f"{_SEP}\n")


if __name__ == "__main__":
    main()
