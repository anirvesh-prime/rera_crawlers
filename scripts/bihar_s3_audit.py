"""List ``rera_projects.key`` values for Bihar rows crawled from a given machine.

Use case:
    The Bihar crawler ran on ``anirveshs-g-2vcpu-8gb-blr1-01`` with the wrong
    key formula and uploaded a parallel set of documents to S3 under those
    (wrong) keys. Per ``SPEC.md`` §7, each project's S3 path prefix is
    ``s3://{bucket}/{key}/``, so the list of keys printed here is also the
    list of S3 prefixes those uploads live under.

Output (default ``./bihar_s3_audit/bihar_s3_audit_project_keys.txt``):
    One ``rera_projects.key`` per line, sorted.

Read-only — never writes to the DB or S3.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.db import get_connection

DEFAULT_STATE = "Bihar"
DEFAULT_MACHINE = "anirveshs-g-2vcpu-8gb-blr1-01"


def fetch_project_keys(conn, state: str, machine_name: str) -> list[str]:
    rows = conn.execute(
        "SELECT key FROM rera_projects "
        "WHERE project_state = %s AND machine_name = %s "
        "ORDER BY key",
        (state, machine_name),
    ).fetchall()
    return [r["key"] for r in rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="List rera_projects.key values for a (state, machine_name) pair.",
    )
    parser.add_argument("--state", default=DEFAULT_STATE,
                        help=f"project_state filter (default: {DEFAULT_STATE!r}).")
    parser.add_argument("--machine-name", default=DEFAULT_MACHINE,
                        help=f"machine_name filter (default: {DEFAULT_MACHINE!r}).")
    parser.add_argument("--output", default="bihar_s3_audit/bihar_s3_audit_project_keys.txt",
                        help="Destination file (default: ./bihar_s3_audit/bihar_s3_audit_project_keys.txt).")
    args = parser.parse_args(argv)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    keys = fetch_project_keys(conn, args.state, args.machine_name)
    with out_path.open("w") as fh:
        for k in keys:
            fh.write(f"{k}\n")

    print(f"state         {args.state}")
    print(f"machine_name  {args.machine_name}")
    print(f"keys matched  {len(keys)}")
    print(f"output        {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
