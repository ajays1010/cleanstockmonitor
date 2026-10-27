"""Apply supabase/migrations/*.sql in file order, each once, recorded in schema_migrations.

Needs DATABASE_URL (Supabase > Connect > Session pooler) in the environment or .env.
Run from the repo root:  python scripts/apply_migrations.py [--dry-run]
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS = ROOT / "supabase" / "migrations"


def pending(conn: psycopg.Connection) -> list[Path]:
    conn.execute(
        "create table if not exists public.schema_migrations ("
        " name text primary key, applied_at timestamptz not null default now())"
    )
    conn.execute("alter table public.schema_migrations enable row level security")
    done = {row[0] for row in conn.execute("select name from public.schema_migrations")}
    return [f for f in sorted(MIGRATIONS.glob("*.sql")) if f.name not in done]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="list pending migrations only")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1

    with psycopg.connect(url, autocommit=True) as conn:
        todo = pending(conn)
        if not todo:
            print("Nothing to apply.")
            return 0
        for path in todo:
            if args.dry_run:
                print(f"pending: {path.name}")
                continue
            # Each file and its record commit together, so a failed file leaves no trace.
            with conn.transaction():
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute("insert into public.schema_migrations (name) values (%s)", (path.name,))
            print(f"applied: {path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
