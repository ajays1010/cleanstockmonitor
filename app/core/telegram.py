"""Minimal Telegram Bot API client."""

import logging
from dataclasses import dataclass

import httpx

from app.core.config import get_settings

log = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"
MAX_MESSAGE_LEN = 4096


@dataclass
class SendResult:
    ok: bool
    error: str | None = None
    retry_after: int | None = None  # seconds, when Telegram rate-limits us


def send_message(chat_id: int, text: str, client: httpx.Client | None = None) -> SendResult:
    token = get_settings().telegram_bot_token.get_secret_value()
    if not token:
        return SendResult(ok=False, error="TELEGRAM_BOT_TOKEN not set")
    body = {
        "chat_id": chat_id,
        "text": text[:MAX_MESSAGE_LEN],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    http = client or httpx.Client(timeout=15)
    try:
        resp = http.post(f"{API_BASE}/bot{token}/sendMessage", json=body)
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        return SendResult(ok=False, error=f"{type(exc).__name__}: {exc}")
    finally:
        if client is None:
            http.close()
    if data.get("ok"):
        return SendResult(ok=True)
    retry_after = (data.get("parameters") or {}).get("retry_after")
    error = data.get("description", f"HTTP {resp.status_code}")
    return SendResult(ok=False, error=error, retry_after=retry_after)
