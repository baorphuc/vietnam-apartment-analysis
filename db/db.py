"""
db.py — Database connection and initialization helper
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "apartment_analytics"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def run_migration(sql_path: str = "schema.sql"):
    """Run schema.sql against the DB."""
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    print("Migration done.")


if __name__ == "__main__":
    run_migration()
