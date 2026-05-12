#!/usr/bin/env python3
"""
clear_logs.py — Erase crawl log rows from the database.

Modes
-----
  --all          TRUNCATE crawl_logs and crawl_document_events (full wipe, resets IDs)
  --before DATE  DELETE rows older than DATE (format: YYYY-MM-DD, e.g. 2026-05-01)

Examples
--------
  python clear_logs.py --all
  python clear_logs.py --before 2026-05-01
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from core.db import get_connection


def _confirm(prompt: str) -> bool:
    answer = input(f"{prompt} [y/N]: ").strip().lower()
    return answer == "y"


def truncate_all(conn) -> None:
    """TRUNCATE both tables and restart their ID sequences."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE crawl_document_events RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE crawl_logs RESTART IDENTITY CASCADE")
    conn.commit()
    print("Done — crawl_logs and crawl_document_events have been wiped and IDs reset.")


def delete_before(conn, cutoff: date) -> None:
    """DELETE rows older than *cutoff* from both tables."""
    cutoff_str = cutoff.isoformat()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM crawl_document_events WHERE created_at < %s",
            (cutoff_str,),
        )
        doc_count = cur.rowcount

        cur.execute(
            "DELETE FROM crawl_logs WHERE logged_at < %s",
            (cutoff_str,),
        )
        log_count = cur.rowcount

    conn.commit()
    print(f"Deleted {log_count} row(s) from crawl_logs.")
    print(f"Deleted {doc_count} row(s) from crawl_document_events.")
    print(f"(cutoff: < {cutoff_str})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clear crawl log rows from the database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Full wipe: TRUNCATE both tables and restart ID sequences.",
    )
    group.add_argument(
        "--before",
        metavar="DATE",
        help="Delete rows older than this date (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    if args.all:
        if not _confirm(
            "⚠️  This will TRUNCATE crawl_logs and crawl_document_events completely. Continue?"
        ):
            print("Aborted.")
            sys.exit(0)
        conn = get_connection()
        truncate_all(conn)

    elif args.before:
        try:
            cutoff = date.fromisoformat(args.before)
        except ValueError:
            print(f"Error: '{args.before}' is not a valid date (expected YYYY-MM-DD).")
            sys.exit(1)

        if not _confirm(
            f"⚠️  Delete all log rows with timestamps before {cutoff}. Continue?"
        ):
            print("Aborted.")
            sys.exit(0)

        conn = get_connection()
        delete_before(conn, cutoff)


if __name__ == "__main__":
    main()
