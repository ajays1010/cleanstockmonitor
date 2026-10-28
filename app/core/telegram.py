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
    file_id: str | None = None  # from send_document: resend the same file with it


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


def _send(method: str, client: httpx.Client | None, timeout: float, **request) -> SendResult:
    try:
        token = _token()
    except TelegramError as exc:
        return SendResult(ok=False, error=str(exc))
    http = client or httpx.Client(timeout=timeout)
    try:
        resp = http.post(f"{API_BASE}/bot{token}/{method}", **request)
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        return SendResult(ok=False, error=f"{type(exc).__name__}: {exc}")
    finally:
        if client is None:
            http.close()
    if data.get("ok"):
        document = (data.get("result") or {}).get("document") or {}
        return SendResult(ok=True, file_id=document.get("file_id"))
    retry_after = (data.get("parameters") or {}).get("retry_after")
    error = data.get("description", f"HTTP {resp.status_code}")
    return SendResult(ok=False, error=error, retry_after=retry_after)


def send_message(chat_id: int, text: str, client: httpx.Client | None = None) -> SendResult:
    body = {
        "chat_id": chat_id,
        "text": text[:MAX_MESSAGE_LEN],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    return _send("sendMessage", client, 15, json=body)


def send_document(
    chat_id: int,
    caption: str,
    *,
    pdf: bytes | None = None,
    filename: str = "filing.pdf",
    file_id: str | None = None,
    client: httpx.Client | None = None,
) -> SendResult:
    """Send a PDF with an HTML caption: upload `pdf`, or resend an earlier upload by `file_id`
    (Telegram keeps the file, so it is uploaded once however many chats get it)."""
    fields = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    if file_id:
        return _send("sendDocument", client, 30, json={**fields, "document": file_id})
    form = {k: str(v) for k, v in fields.items()}
    return _send(
        "sendDocument", client, 120, data=form, files={"document": (filename, pdf, "application/pdf")}
    )


@lru_cache
def bot_username() -> str:
    """The bot's @username, from settings or else asked from Telegram once."""
    configured = get_settings().telegram_bot_username.lstrip("@")
    if configured:
        return configured
    return call("getMe", {})["username"]
