"""Short AI summaries of filing PDFs from Google's Gemini API (plain REST, no SDK).

Gemini reads the PDF itself, so scanned filings and results tables work. Only the
categories in SUMMARY_CATEGORIES are summarised; routine filings are not worth the quota.
"""

import base64
import json
import logging
import re
import time
from dataclasses import dataclass

import httpx

from app.core.config import get_settings

log = logging.getLogger(__name__)

API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
SUMMARY_CATEGORIES = frozenset({"results", "orders", "board_meeting", "corporate_action", "rating"})
MAX_PDF_BYTES = 14 * 1024 * 1024  # inline requests are capped at 20 MB after base64
MAX_POINTS = 4
TONES = ("positive", "neutral", "negative")
RETRY_STATUSES = (429, 500, 503)
RETRY_DELAY = 10

PROMPT = """You are summarising a stock-exchange filing by {company}, an Indian listed company, \
for a shareholder who will read it on a phone.
Filing headline: {headline}

Give 2 to 4 short points (each under 140 characters) with the facts that matter to a \
shareholder, taken only from this document:
- Financial results: revenue, EBITDA or operating profit, and net profit (PAT), each with \
its change against the same quarter last year (YoY) and against the previous quarter (QoQ), \
written like "Revenue ₹19,951 Cr (YoY +39%, QoQ +12%)". Results statements show both \
comparison quarters as columns; work out the percentage from them when it is not stated, \
and leave out a comparison the document does not support. Prefer consolidated figures when \
both standalone and consolidated are given. Also mention exceptional or one-time items and \
any dividend declared. Name quarters by the Indian April-March financial year: the quarter \
ended 30 Sep 2026 is Q2 FY27, 31 Dec 2026 is Q3 FY27, 31 Mar 2027 is Q4 FY27.
- Orders and contracts: client, order value and timeline; compare with annual revenue only \
if the document states it.
- Board meetings: the date and purpose, or the decisions taken.
- Dividends, splits, bonus, buybacks: ratio or amount per share, and the record date.
- Credit ratings: agency, instrument, old and new rating, outlook.
Write amounts in ₹ crore (for example ₹412 Cr). Plain text only, no markdown. Do not \
speculate or give investment advice.

tone: positive, neutral or negative for shareholders, judged only on these facts. Routine \
or procedural filings are neutral."""

SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "points": {"type": "ARRAY", "items": {"type": "STRING"}},
        "tone": {"type": "STRING", "enum": list(TONES)},
    },
    "required": ["points", "tone"],
}


class GeminiError(Exception):
    pass


@dataclass(frozen=True)
class Summary:
    points: tuple[str, ...]
    tone: str

    def as_payload(self) -> dict:
        return {"points": list(self.points), "tone": self.tone}


def configured() -> bool:
    return bool(get_settings().gemini_api_key.get_secret_value())


def _clean(point) -> str:
    """One line of plain text. Gemini sometimes writes ₹ as a literal "\\u20b9" or puts a
    stray control character (e.g. \\x08) before it."""
    text = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), str(point))
    return " ".join(re.sub(r"[\x00-\x1f\x7f]", " ", text).split())


def parse_response(data: dict) -> Summary:
    try:
        parts = data["candidates"][0]["content"]["parts"]
        text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
        result = json.loads(text)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        reason = (data.get("promptFeedback") or {}).get("blockReason") or type(exc).__name__
        raise GeminiError(f"Unusable Gemini response: {reason}") from exc
    points = tuple(t[:200] for t in map(_clean, result.get("points") or []) if t)
    if not points:
        raise GeminiError("Gemini returned no points")
    tone = result.get("tone") if result.get("tone") in TONES else "neutral"
    return Summary(points[:MAX_POINTS], tone)


def summarise(pdf: bytes, company: str, headline: str, client: httpx.Client | None = None) -> Summary:
    settings = get_settings()
    key = settings.gemini_api_key.get_secret_value()
    if not key:
        raise GeminiError("GEMINI_API_KEY not set")
    body = {
        "contents": [
            {
                "parts": [
                    {"inline_data": {"mime_type": "application/pdf", "data": base64.b64encode(pdf).decode()}},
                    {"text": PROMPT.format(company=company or "the company", headline=headline)},
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
            "responseSchema": SCHEMA,
        },
    }
    http = client or httpx.Client(timeout=90)
    try:
        for attempt in range(2):
            try:
                resp = http.post(
                    API_URL.format(model=settings.gemini_model), json=body, headers={"x-goog-api-key": key}
                )
                data = resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                raise GeminiError(f"{type(exc).__name__}: {exc}") from exc
            if resp.status_code not in RETRY_STATUSES or attempt:
                break
            time.sleep(RETRY_DELAY)  # "model is experiencing high demand" is usually brief
    finally:
        if client is None:
            http.close()
    if resp.status_code != 200:
        message = (data.get("error") or {}).get("message", "") if isinstance(data, dict) else ""
        raise GeminiError(f"HTTP {resp.status_code}: {message[:200]}")
    return parse_response(data)
