"""Short AI summaries of filing PDFs from Google's Gemini API (plain REST, no SDK).

Gemini reads the PDF itself, so scanned filings and results tables work. Only the
categories in SUMMARY_CATEGORIES are summarised; routine filings are not worth the quota.
"""

import base64
import json
import logging
from dataclasses import dataclass

import httpx

from app.core.config import get_settings

log = logging.getLogger(__name__)

API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
SUMMARY_CATEGORIES = frozenset({"results", "orders", "board_meeting", "corporate_action", "rating"})
MAX_PDF_BYTES = 14 * 1024 * 1024  # inline requests are capped at 20 MB after base64
MAX_POINTS = 4
TONES = ("positive", "neutral", "negative")

PROMPT = """You are summarising a stock-exchange filing by {company}, an Indian listed company, \
for a shareholder who will read it on a phone.
Filing headline: {headline}

Give 2 to 4 short points (each under 120 characters) with the facts that matter to a \
shareholder, taken only from this document:
- Financial results: revenue, EBITDA or operating profit, net profit (PAT), each with the \
year-on-year change where given; exceptional items; any dividend declared.
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


def parse_response(data: dict) -> Summary:
    try:
        parts = data["candidates"][0]["content"]["parts"]
        text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
        result = json.loads(text)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        reason = (data.get("promptFeedback") or {}).get("blockReason") or type(exc).__name__
        raise GeminiError(f"Unusable Gemini response: {reason}") from exc
    points = tuple(" ".join(str(p).split())[:200] for p in result.get("points") or [] if str(p).strip())
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
        resp = http.post(
            API_URL.format(model=settings.gemini_model), json=body, headers={"x-goog-api-key": key}
        )
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise GeminiError(f"{type(exc).__name__}: {exc}") from exc
    finally:
        if client is None:
            http.close()
    if resp.status_code != 200:
        message = (data.get("error") or {}).get("message", "") if isinstance(data, dict) else ""
        raise GeminiError(f"HTTP {resp.status_code}: {message[:200]}")
    return parse_response(data)
