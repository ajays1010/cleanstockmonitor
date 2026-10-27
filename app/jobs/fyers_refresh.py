"""Morning Fyers token refresh, and Telegram warnings to admins when a login is needed."""

import logging
from datetime import UTC, datetime, timedelta

from app.core.config import get_settings
from app.core.db import get_db
from app.core.fyers_tokens import TokenStore, fyers_client
from app.core.telegram import send_message
from app.jobs.runner import tracked
from app.sources.fyers import FyersError, Tokens, next_expiry

log = logging.getLogger(__name__)

WARN_BEFORE = timedelta(days=2)


def admin_chats() -> list[int]:
    db = get_db()
    admins = [r["id"] for r in db.table("profiles").select("id").eq("is_admin", True).execute().data]
    chats = []
    if admins:
        rows = (
            db.table("telegram_chats")
            .select("chat_id")
            .in_("user_id", admins)
            .eq("is_active", True)
            .execute()
            .data
        )
        chats = [r["chat_id"] for r in rows]
    extra = get_settings().admin_telegram_chat_id
    if extra and extra not in chats:
        chats.append(extra)
    return chats


def notify_admins(text: str) -> None:
    for chat_id in admin_chats():
        result = send_message(chat_id, text)
        if not result.ok:
            log.warning("Admin notice to %s failed: %s", chat_id, result.error)


def _login_hint() -> str:
    return f"Log in again at {get_settings().app_base_url.rstrip('/')}/admin"


@tracked("refresh_fyers_token")
def refresh_fyers_token() -> dict:
    client = fyers_client()
    if not client.configured:
        return {"skipped": "Fyers not configured"}
    store = TokenStore(get_db)
    tokens = store.get()
    if tokens is None:
        return {"skipped": "never connected"}
    now = datetime.now(UTC)
    pin = get_settings().fyers_pin.get_secret_value()

    if (
        not tokens.refresh_token
        or not pin
        or (tokens.refresh_expires_at and tokens.refresh_expires_at <= now)
    ):
        notify_admins(f"⚠️ Fyers needs a new login; prices are coming from Yahoo meanwhile. {_login_hint()}")
        return {"refreshed": False, "reason": "no usable refresh token or PIN"}

    try:
        access = client.refresh(tokens.refresh_token, pin, now)
    except FyersError as exc:
        notify_admins(f"⚠️ Fyers token refresh failed ({exc}). Prices fall back to Yahoo. {_login_hint()}")
        raise
    store.save(Tokens(access, tokens.refresh_token, next_expiry(now), tokens.refresh_expires_at), now)

    if tokens.refresh_expires_at and tokens.refresh_expires_at - now <= WARN_BEFORE:
        days = max((tokens.refresh_expires_at - now).days, 0)
        notify_admins(f"🔑 Fyers login expires in {days or 'under 1'} day(s). {_login_hint()} to renew it.")
    return {"refreshed": True}
