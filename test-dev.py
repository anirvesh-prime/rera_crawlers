import json
import csv
import psycopg2
from psycopg2.extras import RealDictCursor

PROJECT_NAME = "VISTA DELRIO"

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="rera_crawlers",
        user="anirvesh",
        password="YOUR_PASSWORD",  # <-- replace this
        options="-c timezone=UTC",
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM rera_projects_dedicated WHERE project_name = %s;",
                (PROJECT_NAME,)
            )
            rows = cur.fetchall()

        # Save JSON
        with open("rera_projects_dev.json", "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str, ensure_ascii=False)

        print(f"Fetched {len(rows)} row(s)")

    finally:
        conn.close()

if __name__ == "__main__":
    main()