"""BSE corporate announcements for one scrip, from the API behind bseindia.com.

BSE's CDN refuses requests that do not look like the website's own (403 "Access
Denied"), so the headers below copy what the announcements page sends.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx

from app.core.timeutil import IST, now_ist
from app.sources import BROWSER_USER_AGENT

log = logging.getLogger(__name__)

API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
PDF_LIVE = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


class BseError(Exception):
    pass


@dataclass(frozen=True)
class Announcement:
    news_id: str
    bse_code: str
    company: str
    headline: str
    bse_category: str
    bse_subcategory: str
    published_at: datetime  # aware, IST
    pdf_url: str | None
    category: str  # one of app.core.categories.CATEGORIES


_ORDERS = re.compile(r"\b(orders?|contracts?|letter of award|loa|work order)\b")
_NOT_ORDERS = re.compile(r"\bin order to\b|\bcourt order\b|\border of (the )?(nclt|court|tribunal)\b")
_CORP_ACTION = re.compile(r"\b(dividend|bonus|split|sub-division|buy-?back|rights issue|record date)\b")
_INVESTOR = re.compile(r"earnings call|investor presentation|analyst|investor meet|con(\.|ference)? call")


def classify(bse_category: str, subcategory: str, headline: str) -> str:
    """Map BSE's category, subcategory and headline to one of our category keys."""
    cat, sub, text = bse_category.lower(), subcategory.lower(), headline.lower()
    if cat == "result" or "financial result" in sub or "financial result" in text:
        return "results"
    if "rating" in sub or "credit rating" in text:
        return "rating"
    if any(k in sub for k in ("investor", "analyst", "earnings call")) or _INVESTOR.search(text):
        return "investor_meet"
    if "order" in sub or (_ORDERS.search(text) and not _NOT_ORDERS.search(text)):
        return "orders"
    if cat == "corp. action" or _CORP_ACTION.search(text):
        return "corporate_action"
    if cat == "board meeting" or "board meeting" in text:
        return "board_meeting"
    return "other"


def _clean(text: str | None) -> str:
    # BSE doubles apostrophes ("Scrutinizer''s Report").
    return " ".join((text or "").replace("''", "'").split())


def _published(row: dict) -> datetime:
    raw = row.get("DissemDT") or row.get("NEWS_DT") or row.get("DT_TM")
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=IST)
    except (TypeError, ValueError):
        return now_ist()


def parse_rows(rows: list[dict]) -> list[Announcement]:
    out = []
    for row in rows:
        news_id = str(row.get("NEWSID") or "").strip()
        if not news_id:
            continue
        headline = _clean(row.get("HEADLINE") or row.get("NEWSSUB"))
        bse_cat = _clean(row.get("CATEGORYNAME"))
        sub = _clean(row.get("SUBCATNAME"))
        attachment = (row.get("ATTACHMENTNAME") or "").strip()
        out.append(
            Announcement(
                news_id=news_id,
                bse_code=str(row.get("SCRIP_CD") or ""),
                company=_clean(row.get("SLONGNAME")),
                headline=headline,
                bse_category=bse_cat,
                bse_subcategory=sub,
                published_at=_published(row),
                pdf_url=PDF_LIVE + attachment if attachment else None,
                category=classify(bse_cat, sub, headline),
            )
        )
    return out


def fetch_announcements(bse_code: str, client: httpx.Client, days: int = 2) -> list[Announcement]:
    """Announcements for one scrip over the last `days` days (today included)."""
    today = now_ist().date()
    params = {
        "pageno": 1,
        "strCat": "-1",
        "strPrevDate": (today - timedelta(days=days - 1)).strftime("%Y%m%d"),
        "strScrip": bse_code,
        "strSearch": "P",
        "strToDate": today.strftime("%Y%m%d"),
        "strType": "C",
        "subcategory": "-1",
    }
    try:
        resp = client.get(API_URL, params=params, headers=HEADERS, timeout=30)
    except httpx.HTTPError as exc:
        raise BseError(f"{type(exc).__name__}: {exc}") from exc
    if resp.status_code != 200:
        raise BseError(f"HTTP {resp.status_code} for scrip {bse_code}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise BseError(f"Non-JSON response for scrip {bse_code}") from exc
    if not isinstance(data, dict):  # "No Record Found!" comes back as a bare string
        return []
    return parse_rows(data.get("Table") or [])
