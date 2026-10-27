"""Linking Telegram chats to users through one-time codes in bot deep links."""

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from supabase import Client

CODE_TTL = timedelta(minutes=10)


@dataclass(frozen=True)
class LinkedChat:
    id: int
    user_id: str
    chat_id: int
    label: str | None


def new_code() -> str:
    # Deep-link payloads allow A-Z a-z 0-9 _ - and up to 64 characters.
    return secrets.token_urlsafe(16)


def _chat(row: dict) -> LinkedChat:
    return LinkedChat(id=row["id"], user_id=row["user_id"], chat_id=row["chat_id"], label=row.get("label"))


class TelegramLinkStore:
    def __init__(self, db_factory: Callable[[], Client]):
        self._db_factory = db_factory

    @property
    def db(self) -> Client:
        return self._db_factory()

    def create_code(self, user_id: str) -> str:
        """Replace any earlier code of this user with a fresh one."""
        self.db.table("telegram_link_codes").delete().eq("user_id", user_id).execute()
        code = new_code()
        expires = datetime.now(UTC) + CODE_TTL
        self.db.table("telegram_link_codes").insert(
            {"code": code, "user_id": user_id, "expires_at": expires.isoformat()}
        ).execute()
        return code

    def consume_code(self, code: str) -> str | None:
        """Delete the code and return its user, or None if unknown or expired.

        The delete is the claim: two chats racing with one code cannot both get a row back.
        """
        rows = self.db.table("telegram_link_codes").delete().eq("code", code).execute().data
        if not rows:
            return None
        expires = datetime.fromisoformat(rows[0]["expires_at"])
        return rows[0]["user_id"] if expires > datetime.now(UTC) else None

    def link(self, user_id: str, chat_id: int, label: str | None) -> None:
        self.db.table("telegram_chats").upsert(
            {"user_id": user_id, "chat_id": chat_id, "label": label, "is_active": True},
            on_conflict="user_id,chat_id",
        ).execute()

    def chats_for_user(self, user_id: str) -> list[LinkedChat]:
        rows = (
            self.db.table("telegram_chats")
            .select("id, user_id, chat_id, label")
            .eq("user_id", user_id)
            .eq("is_active", True)
            .order("created_at")
            .execute()
            .data
        )
        return [_chat(r) for r in rows]

    def users_for_chat(self, chat_id: int) -> list[str]:
        rows = (
            self.db.table("telegram_chats")
            .select("user_id")
            .eq("chat_id", chat_id)
            .eq("is_active", True)
            .execute()
            .data
        )
        return [r["user_id"] for r in rows]

    def get_for_user(self, user_id: str, link_id: int) -> LinkedChat | None:
        rows = (
            self.db.table("telegram_chats")
            .select("id, user_id, chat_id, label")
            .eq("id", link_id)
            .eq("user_id", user_id)
            .limit(1)
            .execute()
            .data
        )
        return _chat(rows[0]) if rows else None

    def remove_for_user(self, user_id: str, link_id: int) -> None:
        self.db.table("telegram_chats").delete().eq("id", link_id).eq("user_id", user_id).execute()

    def unlink_chat(self, chat_id: int) -> int:
        """Remove every link to this chat (bot removed from a group, or /stop). Returns how many."""
        return len(self.db.table("telegram_chats").delete().eq("chat_id", chat_id).execute().data)
