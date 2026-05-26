#!/usr/bin/env python3
"""Quick 1-item bench: run each enabled state, capture timing + found count."""
import json, os, subprocess, sys, time
from pathlib import Path

PYTHON   = str(Path(__file__).parent / "venv" / "bin" / "python")
ROOT     = Path(__file__).parent
TIMEOUT  = 300  # 5 min hard cap per state

STATES = [
    "kerala_rera", "rajasthan_rera", "odisha_rera", "pondicherry_rera",
    "bihar_rera", "punjab_rera", "maharashtra_rera", "gujarat_rera",
    "karnataka_rera", "haryana_rera", "delhi_rera", "tamil_nadu_rera",
    "jharkhand_rera", "andhra_pradesh_rera", "goa_rera", "tripura_rera",
    "wb_rera", "assam_rera", "himachal_pradesh_rera", "uttarakhand_rera",
    "uttar_pradesh_rera",
]

results = []
grand_start = time.monotonic()

for sid in STATES:
    cmd = [
        PYTHON, "run_crawlers.py",
        "--mode", "weekly_deep",
        "--test",
        "--sequential",
        "--site", sid,
        "--item-limit", "1",
    ]
    env = {**os.environ, "PYTHONHASHSEED": "0", "PYTHONUNBUFFERED": "1"}
    print(f"\n{'='*60}")
    print(f"  {sid}")
    print(f"{'='*60}")
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, cwd=ROOT, env=env, timeout=TIMEOUT,
            capture_output=True, text=True,
        )
        elapsed = time.monotonic() - t0
        status  = "ok" if proc.returncode == 0 else "fail"
        out     = proc.stdout + proc.stderr
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        status  = "timeout"
        out     = ""

    # Parse found/new/updated/errors from the summary line
    found = new = updated = skipped = docs = errors = 0
    for line in out.splitlines():
        if "found=" in line and "new=" in line:
            import re
            for k, v in re.findall(r'(\w+)=(\d+)', line):
                if k == "found":    found    = int(v)
                if k == "new":      new      = int(v)
                if k == "updated":  updated  = int(v)
                if k == "skipped":  skipped  = int(v)
                if k == "docs":     docs     = int(v)
                if k == "errors":   errors   = int(v)

    rec = dict(site=sid, status=status, elapsed_s=round(elapsed, 1),
               found=found, new=new, updated=updated,
               skipped=skipped, docs=docs, errors=errors)
    results.append(rec)

    flag = "✓" if status == "ok" and errors == 0 else ("✗" if status in ("fail","timeout") else "⚠")
    print(f"  {flag}  {elapsed:.1f}s  found={found} new={new} updated={updated} "
          f"skipped={skipped} docs={docs} errors={errors}  [{status}]")

    # Show last few log lines if something went wrong
    if status != "ok" or errors:
        for l in out.splitlines()[-8:]:
            print(f"     | {l}")

print(f"\n\n{'='*60}")
print("  RESULTS")
print(f"{'='*60}")
print(f"  {'State':<30}  {'Time':>6}  {'Status':<8}  {'found':>5}  {'docs':>4}  {'err':>4}")
print(f"  {'-'*30}  {'-'*6}  {'-'*8}  {'-'*5}  {'-'*4}  {'-'*4}")
for r in results:
    flag = "✓" if r["status"] == "ok" and r["errors"] == 0 else "✗"
    print(f"  {flag} {r['site']:<29}  {r['elapsed_s']:>5.1f}s  {r['status']:<8}  "
          f"{r['found']:>5}  {r['docs']:>4}  {r['errors']:>4}")

grand = time.monotonic() - grand_start
print(f"\n  Total bench wall-clock: {grand:.0f}s ({grand/60:.1f} min)")

out_path = ROOT / "_bench_results.json"
out_path.write_text(json.dumps(results, indent=2))
print(f"  Results saved to: {out_path.name}")
