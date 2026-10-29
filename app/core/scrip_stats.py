"""Per-scrip figures from Yahoo's daily history, for volume and DMA alerts: the average
volume of the last 20 completed sessions and the 20/50/200-day moving averages of the
close, stored in scrip_stats."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime, time
from decimal import Decimal

import httpx
from supabase import Client

from app.core.timeutil import now_ist
from app.sources.quotes import DailyBar, fetch_daily_history

log = logging.getLogger(__name__)

DMA_PERIODS = (20, 50, 200)
AVG_VOLUME_SESSIONS = 20
# Yahoo includes the running session in its daily history; it only counts once closed.
SESSION_DONE = time(15, 45)


def completed(bars: list[DailyBar], now: datetime) -> list[DailyBar]:
    today = now.date()
    return [b for b in bars if b.day < today or (b.day == today and now.time() >= SESSION_DONE)]


def compute(bars: list[DailyBar]) -> dict | None:
    """Stats from completed daily bars (oldest first). A figure needs its full window, so a
    recent listing has no 200 DMA until it has traded 200 sessions."""
    if not bars:
        return None
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars[-AVG_VOLUME_SESSIONS:] if b.volume is not None]
    stats: dict = {
        "as_of": bars[-1].day.isoformat(),
        "avg_volume_20": round(sum(volumes) / len(volumes)) if len(volumes) == AVG_VOLUME_SESSIONS else None,
    }
    for n in DMA_PERIODS:
        dma = sum(closes[-n:]) / n if len(closes) >= n else None
        stats[f"dma_{n}"] = str(dma.quantize(Decimal("0.01"))) if dma is not None else None
    return stats


def refresh_one(
    db: Callable[[], Client], scrip_id: int, yahoo_symbol: str | None, http: httpx.Client
) -> bool:
    if not yahoo_symbol:
        return False
    stats = compute(completed(fetch_daily_history(yahoo_symbol, http), now_ist()))
    if stats is None:
        return False
    db().table("scrip_stats").upsert(
        {"scrip_id": scrip_id, **stats, "updated_at": datetime.now(UTC).isoformat()}, on_conflict="scrip_id"
    ).execute()
    return True
