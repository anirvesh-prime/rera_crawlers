import os
import re
import json

import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = "postgresql://postgres:%40bigharddisk1%21@postgres-crawler.hawker.news/crawler"
TABLE_NAME = "rera_projects"
STATE_COLUMN = "state"

OUTPUT_DIR = "state_projects_sample"


def safe_filename(value: str) -> str:
    value = value.strip()
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[-\s]+", "_", value)
    return value.lower()


def fetch_states(conn):
    query = f"""
        SELECT DISTINCT {STATE_COLUMN}
        FROM {TABLE_NAME}
        WHERE {STATE_COLUMN} IS NOT NULL
          AND TRIM({STATE_COLUMN}) <> ''
        ORDER BY {STATE_COLUMN};
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return [row[0] for row in rows]


def fetch_best_project_for_state(conn, state: str):
    """Pick the most populated row for a state.

    Scores each row by how many key JSONB/structured fields are non-null,
    then breaks ties by most recently crawled.  No date filter — we want
    the richest sample regardless of when it was crawled.
    """
    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE {STATE_COLUMN} = %s
        ORDER BY (
            (CASE WHEN uploaded_documents      IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN building_details        IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN construction_progress   IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN project_cost_detail     IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN land_detail             IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN bank_details            IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN promoter_contact_details IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN professional_information IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN status_update           IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN proposed_timeline       IS NOT NULL THEN 1 ELSE 0 END)
        ) DESC,
        last_crawled_date DESC NULLS LAST
        LIMIT 1;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (state,))
        return cur.fetchone()


def save_json(output_dir: str, state: str, record):
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{safe_filename(state)}.json"
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False, default=str)
    return path


def main():
    print("Fetching best-populated sample row per state...")

    conn = psycopg2.connect(DB_URL)
    try:
        states = fetch_states(conn)
        print(f"Found {len(states)} state(s)\n")

        saved = 0
        missing = []

        for state in states:
            record = fetch_best_project_for_state(conn, state)

            if record:
                path = save_json(OUTPUT_DIR, state, record)
                reg = record.get("project_registration_no", "?")
                print(f"[OK] {state} -> {path}  (reg={reg})")
                saved += 1
            else:
                print(f"[SKIP] No record found for state={state}")
                missing.append(state)

        print(f"\nDone — saved {saved} file(s) to '{OUTPUT_DIR}/'")
        if missing:
            print(f"No records at all for: {', '.join(missing)}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()