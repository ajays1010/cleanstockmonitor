"""NSE corporate announcements for one scrip, from the API behind nseindia.com.

Used while BSE cannot be reached from the server (BSE's bot protection blocks cloud
hosts). Most listed companies file the same documents on both exchanges; BSE is
usually a few minutes earlier and has a few more filings, so BSE remains the
preferred source once a home fetcher is available. Filings come back as the same
Announcement records as BSE's, so the rest of the app does not care which it is.
"""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta

import httpx

from app.core.timeutil import IST, now_ist
from app.sources import BROWSER_USER_AGENT
from app.sources.bse import Announcement, classify

log = logging.getLogger(__name__)

HOME_URL = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
API_URL = "https://www.nseindia.com/api/corporate-announcements"
HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Referer": HOME_URL,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
SME_SERIES = ("-SM", "-ST")  # Fyers suffixes of NSE Emerge (SME) symbols


class NseError(Exception):
    pass


def _clean(text: str | None) -> str:
    return " ".join((text or "").split())


def _published(raw: str | None) -> datetime:
    try:
        return datetime.strptime(raw or "", "%d-%b-%Y %H:%M:%S").replace(tzinfo=IST)
    except ValueError:
        return now_ist()


def parse_rows(rows: list[dict], bse_code: str) -> list[Announcement]:
    """NSE rows as Announcements. bse_code ties them to our scrip, which is keyed by it."""
    out = []
    for row in rows:
        seq_id = str(row.get("seq_id") or "").strip()
        if not seq_id:
            continue
        subject = _clean(row.get("desc"))
        text = _clean(row.get("attchmntText")) or subject
        attachment = (row.get("attchmntFile") or "").strip()
        out.append(
            Announcement(
                news_id=seq_id,
                bse_code=bse_code,
                company=_clean(row.get("sm_name")),
                headline=text,
                bse_category="NSE",
                bse_subcategory=subject,
                published_at=_published(row.get("an_dt")),
                pdf_url=attachment if attachment.startswith("https://") else None,
                category=classify("", subject, f"{subject}. {text}"),
            )
        )
    return out


@contextmanager
def client(**kwargs) -> Iterator[httpx.Client]:
    """An HTTP client holding NSE's cookies, which some of its edges insist on."""
    with httpx.Client(timeout=30, headers=HEADERS, follow_redirects=True, **kwargs) as http:
        try:
            http.get(HOME_URL)
        except httpx.HTTPError as exc:
            log.warning("NSE home page: %s", exc)
        yield http


def fetch_announcements(
    symbol: str, bse_code: str, http: httpx.Client, days: int = 2, sme: bool = False
) -> list[Announcement]:
    """Announcements for one NSE symbol over the last `days` days (today included)."""
    today = now_ist().date()
    params = {
        "index": "sme" if sme else "equities",
        "symbol": symbol,
        "from_date": (today - timedelta(days=days - 1)).strftime("%d-%m-%Y"),
        "to_date": today.strftime("%d-%m-%Y"),
    }
    try:
        resp = http.get(API_URL, params=params)
    except httpx.HTTPError as exc:
        raise NseError(f"{type(exc).__name__}: {exc}") from exc
    if resp.status_code != 200:
        raise NseError(f"HTTP {resp.status_code} for {symbol}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise NseError(f"Non-JSON response for {symbol}") from exc
    if isinstance(data, dict):
        data = data.get("data") or []
    return parse_rows(data, bse_code)


def is_sme(fyers_symbol: str | None) -> bool:
    return bool(fyers_symbol) and fyers_symbol.endswith(SME_SERIES)
