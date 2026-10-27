"""Minimal Telegram Bot API client."""

import html
import logging
from dataclasses import dataclass
from functools import lru_cache

import httpx

from app.core.config import get_settings

log = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"
MAX_MESSAGE_LEN = 4096


class TelegramError(Exception):
    pass


@dataclass
class SendResult:
    ok: bool
    error: str | None = None
    retry_after: int | None = None  # seconds, when Telegram rate-limits us


def esc(text: str | None) -> str:
    """Escape text for parse_mode=HTML."""
    return html.escape(text or "", quote=False)


def _token() -> str:
    token = get_settings().telegram_bot_token.get_secret_value()
    if not token:
        raise TelegramError("TELEGRAM_BOT_TOKEN not set")
    return token


def call(method: str, params: dict, client: httpx.Client | None = None, timeout: float = 15) -> dict:
    """Call a Bot API method and return its result, or raise TelegramError."""
    http = client or httpx.Client(timeout=timeout)
    try:
        resp = http.post(f"{API_BASE}/bot{_token()}/{method}", json=params, timeout=timeout)
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise TelegramError(f"{type(exc).__name__}: {exc}") from exc
    finally:
        if client is None:
            http.close()
    if not data.get("ok"):
        raise TelegramError(data.get("description", f"HTTP {resp.status_code}"))
    return data["result"]


def send_message(chat_id: int, text: str, client: httpx.Client | None = None) -> SendResult:
    try:
        token = _token()
    except TelegramError as exc:
        return SendResult(ok=False, error=str(exc))
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


@lru_cache
def bot_username() -> str:
    """The bot's @username, from settings or else asked from Telegram once."""
    configured = get_settings().telegram_bot_username.lstrip("@")
    if configured:
        return configured
    return call("getMe", {})["username"]
