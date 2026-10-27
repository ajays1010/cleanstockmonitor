"""Shared FastAPI dependencies. Tests replace these with app.dependency_overrides."""

from functools import lru_cache
from typing import Annotated
from urllib.parse import quote

from fastapi import Depends, Request

from app.core.config import get_settings
from app.core.db import get_db
from app.core.profiles import Profile, ProfileStore
from app.core.supabase_auth import SupabaseAuth

SESSION_USER_KEY = "uid"


class LoginRequired(Exception):
    def __init__(self, next_path: str):
        self.next_path = next_path


class AdminRequired(Exception):
    pass


@lru_cache
def get_auth() -> SupabaseAuth:
    settings = get_settings()
    return SupabaseAuth(settings.supabase_url, settings.supabase_anon_key.get_secret_value())


def get_profiles() -> ProfileStore:
    return ProfileStore(get_db)


def optional_user(
    request: Request, profiles: Annotated[ProfileStore, Depends(get_profiles)]
) -> Profile | None:
    user_id = request.session.get(SESSION_USER_KEY)
    if not user_id:
        return None
    profile = profiles.get(user_id)
    if profile is None:  # user was deleted: drop the stale session
        request.session.clear()
    return profile


def require_user(request: Request, user: Annotated[Profile | None, Depends(optional_user)]) -> Profile:
    if user is None:
        raise LoginRequired(request.url.path)
    return user


def require_admin(user: Annotated[Profile, Depends(require_user)]) -> Profile:
    if not user.is_admin:
        raise AdminRequired()
    return user


def safe_next(path: str | None) -> str:
    """Only allow redirects to paths on this site, never to another host."""
    if not path or not path.startswith("/") or path.startswith("//") or "\\" in path:
        return "/dashboard"
    return path


def login_url(next_path: str) -> str:
    return f"/login?next={quote(safe_next(next_path), safe='/')}"
