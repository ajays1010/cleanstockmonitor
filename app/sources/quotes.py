"""Delayed stock quotes and daily history from Yahoo's chart API. Fyers quotes come first
when it is connected (app/core/quote_source.py).

Yahoo keeps returning the last session's numbers on holidays and weekends, so each
quote carries its own timestamp and callers ignore quotes that are not from today.
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import httpx

from app.core.timeutil import IST
from app.sources import BROWSER_USER_AGENT

log = logging.getLogger(__name__)

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


@dataclass(frozen=True)
class Quote:
    ltp: Decimal
    prev_close: Decimal | None
    day_high: Decimal | None
    day_low: Decimal | None
    volume: int | None
    quoted_at: datetime  # aware, IST
    source: str

    @property
    def change_pct(self) -> Decimal | None:
        if not self.prev_close:
            return None
        return (self.ltp - self.prev_close) / self.prev_close * 100


def _dec(value) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def parse_yahoo(meta: dict) -> Quote | None:
    price = meta.get("regularMarketPrice")
    stamp = meta.get("regularMarketTime")
    if price is None or stamp is None:
        return None
    return Quote(
        ltp=Decimal(str(price)),
        # With range=1d, chartPreviousClose is the previous session's close.
        prev_close=_dec(meta.get("previousClose") or meta.get("chartPreviousClose")),
        day_high=_dec(meta.get("regularMarketDayHigh")),
        day_low=_dec(meta.get("regularMarketDayLow")),
        volume=meta.get("regularMarketVolume"),
        quoted_at=datetime.fromtimestamp(stamp, tz=IST),
        source="yahoo",
    )


def fetch_yahoo(symbol: str, client: httpx.Client) -> Quote | None:
    try:
        resp = client.get(
            YAHOO_CHART.format(symbol=symbol),
            params={"interval": "1d", "range": "1d"},
            headers={"User-Agent": BROWSER_USER_AGENT},
            timeout=20,
        )
        if resp.status_code != 200:
            log.warning("Yahoo %s: HTTP %s", symbol, resp.status_code)
            return None
        result = (resp.json().get("chart") or {}).get("result") or []
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("Yahoo %s: %s", symbol, exc)
        return None
    return parse_yahoo(result[0].get("meta") or {}) if result else None


@dataclass(frozen=True)
class DailyBar:
    day: date
    close: Decimal
    volume: int | None


def parse_daily(result: dict) -> list[DailyBar]:
    """Yahoo chart result (interval=1d) as daily bars, oldest first; days without a close are skipped."""
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes, volumes = quote.get("close") or [], quote.get("volume") or []
    bars = []
    for i, stamp in enumerate(result.get("timestamp") or []):
        close = closes[i] if i < len(closes) else None
        if close is None:
            continue
        volume = volumes[i] if i < len(volumes) else None
        bars.append(DailyBar(datetime.fromtimestamp(stamp, tz=IST).date(), Decimal(str(close)), volume))
    return bars


def fetch_daily_history(symbol: str, client: httpx.Client, period: str = "1y") -> list[DailyBar]:
    try:
        resp = client.get(
            YAHOO_CHART.format(symbol=symbol),
            params={"interval": "1d", "range": period},
            headers={"User-Agent": BROWSER_USER_AGENT},
            timeout=30,
        )
        if resp.status_code != 200:
            log.warning("Yahoo history %s: HTTP %s", symbol, resp.status_code)
            return []
        result = (resp.json().get("chart") or {}).get("result") or []
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("Yahoo history %s: %s", symbol, exc)
        return []
    return parse_daily(result[0]) if result else []
