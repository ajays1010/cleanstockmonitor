"""Read and create rows in profiles (one per Supabase Auth user)."""

from collections.abc import Callable
from dataclasses import dataclass

from supabase import Client

from app.core.supabase_auth import AuthUser

COLUMNS = "id, email, display_name, is_admin"


@dataclass(frozen=True)
class Profile:
    id: str
    email: str | None
    display_name: str | None
    is_admin: bool


class ProfileStore:
    def __init__(self, db_factory: Callable[[], Client]):
        # Connect lazily, so pages that never look up a user work without database settings.
        self._db_factory = db_factory

    @property
    def db(self) -> Client:
        return self._db_factory()

    def get(self, user_id: str) -> Profile | None:
        rows = self.db.table("profiles").select(COLUMNS).eq("id", user_id).limit(1).execute().data
        return _profile(rows[0]) if rows else None

    def ensure(self, user: AuthUser) -> Profile:
        """Return the user's profile, creating it if the sign-up trigger has not."""
        existing = self.get(user.id)
        if existing:
            return existing
        rows = (
            self.db.table("profiles")
            .upsert({"id": user.id, "email": user.email, "display_name": user.name}, on_conflict="id")
            .execute()
            .data
        )
        return _profile(rows[0])


def _profile(row: dict) -> Profile:
    return Profile(
        id=row["id"],
        email=row.get("email"),
        display_name=row.get("display_name"),
        is_admin=bool(row.get("is_admin")),
    )
