"""Fyers tokens in provider_tokens, plus a ready-to-use client for the configured app."""

from collections.abc import Callable
from datetime import datetime

from supabase import Client

from app.core.config import get_settings
from app.sources.fyers import FyersClient, Tokens

PROVIDER = "fyers"


def fyers_client() -> FyersClient:
    s = get_settings()
    redirect = s.fyers_redirect_uri or s.app_base_url.rstrip("/") + "/admin/fyers/callback"
    return FyersClient(s.fyers_app_id, s.fyers_secret.get_secret_value(), redirect)


class TokenStore:
    def __init__(self, db_factory: Callable[[], Client]):
        self._db_factory = db_factory

    @property
    def db(self) -> Client:
        return self._db_factory()

    def get(self) -> Tokens | None:
        rows = self.db.table("provider_tokens").select("*").eq("provider", PROVIDER).limit(1).execute().data
        if not rows or not rows[0].get("access_token"):
            return None
        r = rows[0]
        return Tokens(
            access_token=r["access_token"], access_expires_at=datetime.fromisoformat(r["access_expires_at"])
        )

    def access_token(self, now: datetime) -> str | None:
        """The current access token, or None if there is none or it has expired."""
        tokens = self.get()
        return tokens.access_token if tokens and tokens.access_expires_at > now else None

    def save(self, tokens: Tokens, now: datetime) -> None:
        self.db.table("provider_tokens").upsert(
            {
                "provider": PROVIDER,
                "access_token": tokens.access_token,
                "access_expires_at": tokens.access_expires_at.isoformat(),
                # Unused since Fyers disabled refreshing; cleared so no stale credential lingers.
                "refresh_token": None,
                "refresh_expires_at": None,
                "updated_at": now.isoformat(),
            },
            on_conflict="provider",
        ).execute()

    def clear(self) -> None:
        self.db.table("provider_tokens").delete().eq("provider", PROVIDER).execute()
