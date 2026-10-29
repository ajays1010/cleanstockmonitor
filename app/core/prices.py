"""Prices for the dashboard, read from price_snapshots (kept fresh by the price job).

Opening the dashboard calls a quote provider only for the Nifty 50 strip, and at most
once a minute (cached per process), plus a single quote when a scrip is added, so its
card has a price before the next 5-minute poll.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import httpx
from supabase import Client

from app.core.quote_source import fetch_quotes, snapshot_row
from app.core.scrip_stats import DMA_PERIODS, refresh_one
from app.core.timeutil import IST, is_market_open, now_ist

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceView:
    ltp: Decimal
    change_pct: Decimal | None
    label: str  # "as of 15:25" during the session, else "Close, 27 Oct"
    # (period, level, % the price is above (+) or below (-) it), for the DMAs known so far
    dmas: tuple[tuple[int, Decimal, Decimal], ...] = ()


def price_view(snapshot: dict, now: datetime, stats: dict | None = None) -> PriceView:
    ltp = Decimal(str(snapshot["ltp"]))
    prev = snapshot.get("prev_close")
    change = (ltp - Decimal(str(prev))) / Decimal(str(prev)) * 100 if prev else None
    quoted = datetime.fromisoformat(snapshot["quoted_at"]).astimezone(IST)
    live = quoted.date() == now.date() and is_market_open(now, set())
    label = f"as of {quoted:%H:%M}" if live else f"Close, {quoted:%d %b}"
    dmas = []
    for n in DMA_PERIODS:
        if raw := (stats or {}).get(f"dma_{n}"):
            level = Decimal(str(raw))
            dmas.append((n, level, (ltp - level) / level * 100))
    return PriceView(ltp=ltp, change_pct=change, label=label, dmas=tuple(dmas))


class PriceStore:
    def __init__(self, db_factory: Callable[[], Client]):
        self._db_factory = db_factory

    @property
    def db(self) -> Client:
        return self._db_factory()

    def views(self, scrip_ids: list[int]) -> dict[int, PriceView]:
        if not scrip_ids:
            return {}
        rows = self.db.table("price_snapshots").select("*").in_("scrip_id", scrip_ids).execute().data
        stats = {
            r["scrip_id"]: r
            for r in self.db.table("scrip_stats").select("*").in_("scrip_id", scrip_ids).execute().data
        }
        now = now_ist()
        return {r["scrip_id"]: price_view(r, now, stats.get(r["scrip_id"])) for r in rows}

    def refresh(self, scrip_id: int) -> None:
        """Fetch one quote and the scrip's DMAs and store them. Never raises: a missing price
        must not block adding."""
        try:
            rows = (
                self.db.table("scrips")
                .select("id, yahoo_symbol, fyers_symbol")
                .eq("id", scrip_id)
                .limit(1)
                .execute()
                .data
            )
            quote = fetch_quotes(rows).get(scrip_id) if rows else None
            if quote is not None:
                self.db.table("price_snapshots").upsert(
                    snapshot_row(scrip_id, quote), on_conflict="scrip_id"
                ).execute()
            if rows:
                with httpx.Client() as http:
                    refresh_one(self._db_factory, scrip_id, rows[0].get("yahoo_symbol"), http)
        except Exception:
            log.exception("Could not refresh the price of scrip %s", scrip_id)

    def nifty(self) -> PriceView | None:
        return nifty_view(now_ist())


NIFTY = {"id": 0, "yahoo_symbol": "^NSEI", "fyers_symbol": "NSE:NIFTY50-INDEX"}
NIFTY_TTL_LIVE = 60  # seconds; the dashboard polls every 60 s in the session
NIFTY_TTL_CLOSED = 30 * 60
_nifty_cache: tuple[float, dict | None] = (0.0, None)  # (monotonic fetch time, snapshot)


def nifty_view(now: datetime, fetch: Callable[[list[dict]], dict] = fetch_quotes) -> PriceView | None:
    """The Nifty 50 for the dashboard strip. A failed fetch is cached too, so a provider
    outage costs one call a minute rather than one per page view."""
    global _nifty_cache
    fetched_at, snapshot = _nifty_cache
    ttl = NIFTY_TTL_LIVE if is_market_open(now, set()) else NIFTY_TTL_CLOSED
    if snapshot is None or time.monotonic() - fetched_at > ttl:
        try:
            quote = fetch([NIFTY]).get(NIFTY["id"])
        except Exception:
            log.exception("Could not fetch the Nifty 50")
            quote = None
        snapshot = snapshot_row(NIFTY["id"], quote) if quote else snapshot
        _nifty_cache = (time.monotonic(), snapshot)
    return price_view(snapshot, now) if snapshot else None
