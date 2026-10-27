import pytest

from app.core.config import Settings


def test_blank_optional_values_are_unset(monkeypatch):
    monkeypatch.setenv("ADMIN_TELEGRAM_CHAT_ID", "")
    assert Settings(_env_file=None).admin_telegram_chat_id is None


def test_production_requires_secrets():
    settings = Settings(_env_file=None, app_env="production")
    with pytest.raises(RuntimeError, match="SUPABASE_URL"):
        settings.check_production()


def test_development_does_not_require_secrets():
    Settings(_env_file=None).check_production()
