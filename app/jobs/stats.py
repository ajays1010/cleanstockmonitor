"""Refresh scrip_stats (20-day average volume, 20/50/200 DMAs) for watched scrips; runs
at 08:30 and 18:30 IST. New scrips get theirs when added (app/core/prices.py)."""

import logging

import httpx

from app.core.db import get_db
from app.core.scrip_stats import refresh_one
from app.jobs.market import watched_scrips
from app.jobs.runner import tracked

log = logging.getLogger(__name__)


@tracked("update_scrip_stats")
def update_scrip_stats() -> dict:
    scrips = watched_scrips()
    updated, failed = 0, []
    with httpx.Client() as http:
        for s in scrips:
            try:
                updated += refresh_one(get_db, s["id"], s.get("yahoo_symbol"), http)
            except Exception:  # one scrip's failure must not cost the others their stats
                log.exception("Stats for scrip %s failed", s["id"])
                failed.append(s["id"])
    if failed and not updated:
        raise RuntimeError(f"Stats failed for all {len(failed)} scrips")
    return {"scrips": len(scrips), "updated": updated, "failed": failed}
