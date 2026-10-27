"""Settings read from environment variables (or a .env file in development)."""

from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # env_ignore_empty: a blank line like ADMIN_TELEGRAM_CHAT_ID= means "not set", not an invalid int.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", env_ignore_empty=True)

    app_env: str = "development"
    app_base_url: str = "http://localhost:8001"
    log_level: str = "INFO"

    supabase_url: str = ""
    supabase_anon_key: SecretStr = SecretStr("")
    supabase_service_key: SecretStr = SecretStr("")

    session_secret: SecretStr = SecretStr("")

    telegram_bot_token: SecretStr = SecretStr("")
    telegram_bot_username: str = ""
    admin_telegram_chat_id: int | None = None

    fyers_app_id: str = ""
    fyers_secret: SecretStr = SecretStr("")
    fyers_pin: SecretStr = SecretStr("")

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    def check_production(self) -> None:
        """Fail fast at startup if a production deploy is missing required secrets."""
        if not self.is_production:
            return
        required = {
            "SUPABASE_URL": self.supabase_url,
            "SUPABASE_SERVICE_KEY": self.supabase_service_key.get_secret_value(),
            "SESSION_SECRET": self.session_secret.get_secret_value(),
            "TELEGRAM_BOT_TOKEN": self.telegram_bot_token.get_secret_value(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError(f"Missing required settings: {', '.join(missing)}")


@lru_cache
def get_settings() -> Settings:
    return Settings()
