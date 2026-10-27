"""Scrip search and each user's watchlist with its alert settings."""

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from supabase import Client

from app.core.categories import CATEGORIES

MAX_WATCHLIST = 50
SPIKE_MIN, SPIKE_MAX = Decimal("0.5"), Decimal("50")
DEFAULT_SPIKE = Decimal("5")

# PostgREST filter syntax treats , ( ) * : . and quotes specially; keep only plain search characters.
# Dropping . and ' also matches how the scrip list spells names ("Dr Reddys", not "Dr. Reddy's").
_UNSAFE = re.compile(r"[^0-9A-Za-z &\-]")
_SPACES = re.compile(r"\s+")


class WatchlistError(Exception):
    """A user-facing problem with a watchlist change."""


@dataclass(frozen=True)
class Scrip:
    id: int
    bse_code: str
    name: str
    nse_symbol: str | None
    bse_symbol: str | None = None


@dataclass(frozen=True)
class WatchItem:
    id: int
    scrip: Scrip
    spike_pct: Decimal | None
    price_above: Decimal | None
    price_below: Decimal | None
    categories: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AlertSettings:
    spike_pct: Decimal | None
    price_above: Decimal | None
    price_below: Decimal | None
    categories: list[str]


def parse_settings(
    spike_pct: str, price_above: str, price_below: str, categories: list[str]
) -> AlertSettings:
    """Validate form input. Blank spike or price fields mean that alert is off."""
    spike = _decimal(spike_pct, "Move alert")
    if spike is not None and not SPIKE_MIN <= spike <= SPIKE_MAX:
        raise WatchlistError(f"Move alert must be between {SPIKE_MIN}% and {SPIKE_MAX}%.")
    above = _decimal(price_above, "Price above")
    below = _decimal(price_below, "Price below")
    for label, value in (("Price above", above), ("Price below", below)):
        if value is not None and value <= 0:
            raise WatchlistError(f"{label} must be more than 0.")
    if above is not None and below is not None and below >= above:
        raise WatchlistError("Price below must be lower than price above.")
    unknown = set(categories) - CATEGORIES.keys()
    if unknown:
        raise WatchlistError("Unknown announcement category.")
    if not categories:
        # An empty filter is stored to mean "all", so "none" has to be refused, not saved.
        raise WatchlistError("Pick at least one announcement type, or remove the scrip.")
    # Ticking every category is the same as no filter; store it as empty.
    chosen = [] if set(categories) == CATEGORIES.keys() else [c for c in CATEGORIES if c in categories]
    return AlertSettings(spike, above, below, chosen)


def _decimal(raw: str, label: str) -> Decimal | None:
    raw = (raw or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        value = Decimal(raw)
    except InvalidOperation:
        raise WatchlistError(f"{label} must be a number.") from None
    if not value.is_finite():
        raise WatchlistError(f"{label} must be a number.")
    return value.quantize(Decimal("0.01"))


def _num(value) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _scrip(row: dict) -> Scrip:
    return Scrip(
        id=row["id"],
        bse_code=row["bse_code"],
        name=row["name"],
        nse_symbol=row.get("nse_symbol"),
        bse_symbol=row.get("bse_symbol"),
    )


def rank_matches(query: str, scrips: list[Scrip], limit: int) -> list[Scrip]:
    """Exact code or symbol first, then names starting with the query, then the rest."""
    q = query.lower()

    def score(s: Scrip) -> tuple[int, str]:
        symbol = (s.nse_symbol or "").lower()
        bse_symbol = (s.bse_symbol or "").lower()
        name = s.name.lower()
        if q in (s.bse_code, symbol, bse_symbol):
            rank = 0
        elif symbol.startswith(q) or bse_symbol.startswith(q) or name.startswith(q):
            rank = 1
        else:
            rank = 2
        return rank, name

    return sorted(scrips, key=score)[:limit]


class WatchlistStore:
    def __init__(self, db_factory: Callable[[], Client]):
        self._db_factory = db_factory

    @property
    def db(self) -> Client:
        return self._db_factory()

    def search(self, query: str, limit: int = 10) -> list[Scrip]:
        q = _SPACES.sub(" ", _UNSAFE.sub("", query)).strip()
        if len(q) < 2:
            return []
        filters = f"name.ilike.*{q}*,nse_symbol.ilike.{q}*,bse_symbol.ilike.{q}*,bse_code.like.{q}*"
        rows = (
            self.db.table("scrips")
            .select("id, bse_code, name, nse_symbol, bse_symbol")
            .eq("is_active", True)
            .or_(filters)
            .limit(40)
            .execute()
            .data
        )
        return rank_matches(q, [_scrip(r) for r in rows], limit)

    def list(self, user_id: str) -> list[WatchItem]:
        rows = (
            self.db.table("watchlist")
            .select(
                "id, spike_pct, price_above, price_below, categories, "
                "scrips(id, bse_code, name, nse_symbol, bse_symbol)"
            )
            .eq("user_id", user_id)
            .execute()
            .data
        )
        items = [
            WatchItem(
                id=r["id"],
                scrip=_scrip(r["scrips"]),
                spike_pct=_num(r.get("spike_pct")),
                price_above=_num(r.get("price_above")),
                price_below=_num(r.get("price_below")),
                categories=r.get("categories") or [],
            )
            for r in rows
        ]
        return sorted(items, key=lambda i: i.scrip.name.lower())

    def add(self, user_id: str, scrip_id: int) -> None:
        current = self.list(user_id)
        if any(i.scrip.id == scrip_id for i in current):
            return
        if len(current) >= MAX_WATCHLIST:
            raise WatchlistError(f"You can watch up to {MAX_WATCHLIST} scrips.")
        exists = self.db.table("scrips").select("id").eq("id", scrip_id).limit(1).execute().data
        if not exists:
            raise WatchlistError("That scrip was not found.")
        self.db.table("watchlist").upsert(
            {"user_id": user_id, "scrip_id": scrip_id, "spike_pct": str(DEFAULT_SPIKE)},
            on_conflict="user_id,scrip_id",
            ignore_duplicates=True,
        ).execute()

    def remove(self, user_id: str, item_id: int) -> None:
        # Filtering on user_id too means a user can never delete someone else's row.
        self.db.table("watchlist").delete().eq("id", item_id).eq("user_id", user_id).execute()

    def update(self, user_id: str, item_id: int, settings: AlertSettings) -> None:
        rows = (
            self.db.table("watchlist")
            .update(
                {
                    "spike_pct": _str(settings.spike_pct),
                    "price_above": _str(settings.price_above),
                    "price_below": _str(settings.price_below),
                    "categories": settings.categories,
                }
            )
            .eq("id", item_id)
            .eq("user_id", user_id)
            .execute()
            .data
        )
        if not rows:
            raise WatchlistError("That watchlist entry was not found.")


def _str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
