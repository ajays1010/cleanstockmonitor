"""Static checks on the SQL migrations; they run without a database."""

import re
from pathlib import Path

from scripts.build_scrip_seed import build_rows

MIGRATIONS = Path(__file__).resolve().parent.parent / "supabase" / "migrations"


def test_every_table_has_row_level_security():
    sql = (MIGRATIONS / "0001_schema.sql").read_text(encoding="utf-8")
    tables = set(re.findall(r"^create table (\w+)", sql, flags=re.M))
    secured = set(re.findall(r"^alter table (\w+) enable row level security", sql, flags=re.M))
    assert tables, "no tables found"
    assert tables == secured


def test_deliveries_are_unique_per_event_and_chat():
    sql = (MIGRATIONS / "0001_schema.sql").read_text(encoding="utf-8")
    deliveries = sql.split("create table deliveries", 1)[1].split(");", 1)[0]
    assert "unique (event_id, chat_id)" in deliveries


def test_seed_is_up_to_date_and_unique():
    rows = build_rows()
    codes = [r[0] for r in rows]
    assert len(codes) == len(set(codes))
    seed = (MIGRATIONS / "0002_seed_scrips.sql").read_text(encoding="utf-8")
    assert seed.count("\n  ('") == len(rows), "re-run scripts/build_scrip_seed.py"
