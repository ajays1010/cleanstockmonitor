"""Telegram messages to the app's admins (warnings from the watchdog, Fyers reminders)."""

import logging

from app.core.config import get_settings
from app.core.db import get_db
from app.core.telegram import send_message

log = logging.getLogger(__name__)


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
