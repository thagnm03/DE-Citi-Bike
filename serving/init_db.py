from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[1]


def initialize(database_url: str, *, reset: bool = False) -> None:
    schema_sql = (ROOT / "serving" / "schema.sql").read_text(encoding="utf-8")
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            if reset:
                cursor.execute("DROP SCHEMA IF EXISTS serving CASCADE")
            cursor.execute(schema_sql)


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize the Citi Bike serving schema")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", "postgresql://citibike:citibike_local_only@postgres:5432/citibike"),
    )
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    initialize(args.database_url, reset=args.reset)
    print("Serving schema initialized.")


if __name__ == "__main__":
    main()
