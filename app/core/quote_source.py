"""One place that decides where prices come from: Fyers when it is connected, Yahoo for
anything Fyers did not return (not connected, token expired, no Fyers symbol, an error)."""

import logging
from datetime import UTC, datetime

import httpx

from app.core.db import get_db
from app.core.fyers_tokens import TokenStore, fyers_client
from app.sources.fyers import FyersError
from app.sources.quotes import Quote, fetch_yahoo

log = logging.getLogger(__name__)


def fetch_quotes(scrips: list[dict]) -> dict[int, Quote]:
    """scrip_id -> Quote for each scrip a source answered for."""
    out: dict[int, Quote] = {}
    client = fyers_client()
    token = TokenStore(get_db).access_token(datetime.now(UTC)) if client.configured else None
    if token:
        by_symbol = {s["fyers_symbol"]: s["id"] for s in scrips if s.get("fyers_symbol")}
        try:
            for symbol, quote in client.quotes(list(by_symbol), token).items():
                if symbol in by_symbol:
                    out[by_symbol[symbol]] = quote
        except FyersError as exc:
            log.warning("Fyers quotes failed, using Yahoo: %s", exc)
    missing = [s for s in scrips if s["id"] not in out and s.get("yahoo_symbol")]
    if missing:
        with httpx.Client() as http:
            for s in missing:
                quote = fetch_yahoo(s["yahoo_symbol"], http)
                if quote:
                    out[s["id"]] = quote
    return out


def snapshot_row(scrip_id: int, q: Quote) -> dict:
    return {
        "scrip_id": scrip_id,
        "ltp": str(q.ltp),
        "prev_close": str(q.prev_close) if q.prev_close is not None else None,
        "day_high": str(q.day_high) if q.day_high is not None else None,
        "day_low": str(q.day_low) if q.day_low is not None else None,
        "volume": q.volume,
        "source": q.source,
        "quoted_at": q.quoted_at.isoformat(),
    }
