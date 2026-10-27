"""Supabase client. The backend always uses the service role key; the browser never talks to Supabase."""

from functools import lru_cache

from supabase import Client, create_client

from app.core.config import get_settings


@lru_cache
def get_db() -> Client:
    settings = get_settings()
    key = settings.supabase_service_key.get_secret_value()
    if not settings.supabase_url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    return create_client(settings.supabase_url, key)
