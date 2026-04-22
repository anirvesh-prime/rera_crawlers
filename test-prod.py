import json
import csv
import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = "postgresql://postgres:%40bigharddisk1%21@postgres-crawler.hawker.news/crawler"
PROJECT_NAME = "VISTA DELRIO"

def main():
    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM rera_projects WHERE project_name = %s;",
                (PROJECT_NAME,)
            )
            rows = cur.fetchall()

        # Write JSON
        with open("rera_projects_prod.json", "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str, ensure_ascii=False)

        print(f"Fetched {len(rows)} row(s)")
        print("Saved to rera_projects_prod.json")

    finally:
        conn.close()

if __name__ == "__main__":
    main()