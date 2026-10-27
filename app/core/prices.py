"""Prices for the dashboard, read from price_snapshots (kept fresh by the price job).

Opening the dashboard never calls a quote provider. The only outside call is a single
quote when a scrip is added, so its card has a price before the next 5-minute poll.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import httpx
from supabase import Client

from app.core.timeutil import IST, is_market_open, now_ist
from app.sources.quotes import fetch_yahoo

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceView:
    ltp: Decimal
    change_pct: Decimal | None
    label: str  # "as of 15:25" during the session, else "Close, 27 Oct"


def price_view(snapshot: dict, now: datetime) -> PriceView:
    ltp = Decimal(str(snapshot["ltp"]))
    prev = snapshot.get("prev_close")
    change = (ltp - Decimal(str(prev))) / Decimal(str(prev)) * 100 if prev else None
    quoted = datetime.fromisoformat(snapshot["quoted_at"]).astimezone(IST)
    live = quoted.date() == now.date() and is_market_open(now, set())
    label = f"as of {quoted:%H:%M}" if live else f"Close, {quoted:%d %b}"
    return PriceView(ltp=ltp, change_pct=change, label=label)


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
        now = now_ist()
        return {r["scrip_id"]: price_view(r, now) for r in rows}

    def refresh(self, scrip_id: int) -> None:
        """Fetch one quote and store it. Never raises: a missing price must not block adding."""
        try:
            rows = self.db.table("scrips").select("yahoo_symbol").eq("id", scrip_id).limit(1).execute().data
            symbol = rows[0]["yahoo_symbol"] if rows else None
            if not symbol:
                return
            with httpx.Client() as http:
                quote = fetch_yahoo(symbol, http)
            if quote is None:
                return
            self.db.table("price_snapshots").upsert(
                {
                    "scrip_id": scrip_id,
                    "ltp": str(quote.ltp),
                    "prev_close": str(quote.prev_close) if quote.prev_close is not None else None,
                    "day_high": str(quote.day_high) if quote.day_high is not None else None,
                    "day_low": str(quote.day_low) if quote.day_low is not None else None,
                    "volume": quote.volume,
                    "source": quote.source,
                    "quoted_at": quote.quoted_at.isoformat(),
                },
                on_conflict="scrip_id",
            ).execute()
        except Exception:
            log.exception("Could not refresh the price of scrip %s", scrip_id)
