"""Refresh the scrips table from the exchanges. Runs daily; run by hand with
`python -m app.jobs.scrips`.

New listings are added, names and symbols updated, and scrips that left BSE's active
list are marked inactive (hidden from search; existing watchlist rows keep working).
"""

import logging
from dataclasses import replace
from datetime import UTC, datetime, time, timedelta

import httpx

from app.core.db import get_db
from app.core.timeutil import IST
from app.jobs.runner import tracked
from app.sources.fyers import fetch_symbol_master
from app.sources.scrip_master import ScripRow, fetch_all

log = logging.getLogger(__name__)

BATCH = 500
# New listings appear in the exchange lists the evening before or the morning of listing.
SYNC_SLOTS = (time(6, 30), time(20, 30))  # IST


def with_fyers_symbols(rows: list[ScripRow], nse: dict[str, str], bse: dict[str, str]) -> list[ScripRow]:
    """Fyers ticker per scrip: NSE (main board or SME) by ISIN where it trades there, else BSE by code."""
    return [
        replace(r, fyers_symbol=(nse.get(r.isin) if r.isin else None) or bse.get(r.bse_code)) for r in rows
    ]


def sync_due(now: datetime, last: datetime | None) -> bool:
    """True when a sync slot (06:30 or 20:30 IST) has passed since the last successful sync."""
    slots = [
        datetime.combine(now.date() - timedelta(days=d), slot, tzinfo=IST)
        for d in (0, 1)
        for slot in SYNC_SLOTS
    ]
    latest = max(s for s in slots if s <= now)
    return last is None or last < latest


@tracked("sync_scrips")
def sync_scrips(bse_rows: list[dict] | None = None) -> dict:
    """bse_rows: BSE's scrip list when it was fetched elsewhere (the home relay)."""
    with httpx.Client() as http:
        rows = fetch_all(http, bse_rows)
        try:
            rows = with_fyers_symbols(rows, *fetch_symbol_master(http))
        except httpx.HTTPError as exc:  # keep the exchange data; Fyers symbols stay as derived
            log.warning("Fyers symbol master unavailable: %s", exc)
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
        "with_fyers": sum(1 for r in rows if r.fyers_symbol),
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
