"""Refresh the scrips table from the exchanges. Runs daily; run by hand with
`python -m app.jobs.scrips`.

New listings are added, names and symbols updated, and scrips that left BSE's active
list are marked inactive (hidden from search; existing watchlist rows keep working).
"""

from datetime import UTC, datetime

import httpx

from app.core.db import get_db
from app.jobs.runner import tracked
from app.sources.scrip_master import fetch_all

BATCH = 500


@tracked("sync_scrips")
def sync_scrips() -> dict:
    with httpx.Client() as http:
        rows = fetch_all(http)
    db = get_db()
    existing = set()
    page = 0
    while True:  # PostgREST returns at most 1000 rows per request
        chunk = db.table("scrips").select("bse_code").range(page * 1000, page * 1000 + 999).execute().data
        existing.update(r["bse_code"] for r in chunk)
        if len(chunk) < 1000:
            break
        page += 1

    now = datetime.now(UTC).isoformat()
    payload = [{**r.as_db(), "updated_at": now} for r in rows]
    for i in range(0, len(payload), BATCH):
        db.table("scrips").upsert(payload[i : i + BATCH], on_conflict="bse_code").execute()

    active = {r.bse_code for r in rows}
    gone = sorted(existing - active)
    for i in range(0, len(gone), BATCH):
        db.table("scrips").update({"is_active": False, "updated_at": now}).in_(
            "bse_code", gone[i : i + BATCH]
        ).execute()
    added = [r for r in rows if r.bse_code not in existing]
    return {
        "active": len(rows),
        "added": len(added),
        "deactivated": len(gone),
        "with_nse": sum(1 for r in rows if r.nse_symbol),
        "sample_added": [r.name for r in added[:10]],
    }


if __name__ == "__main__":
    from app.core.logging import setup_logging

    setup_logging()
    sync_scrips()
    print(
        get_db()
        .table("job_runs")
        .select("ok, stats, error")
        .order("id", desc=True)
        .limit(1)
        .execute()
        .data[0]
    )
