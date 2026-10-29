"""Morning nudge to connect Fyers. Fyers disabled its refresh-token API to comply with
SEBI's rules, so each trading day's session starts with the admin logging in (OTP and PIN
on Fyers' page). Without that, prices simply come from Yahoo for the day."""

from app.core.config import get_settings
from app.core.db import get_db
from app.core.fyers_tokens import TokenStore, fyers_client
from app.core.timeutil import now_ist
from app.jobs.market import market_holidays
from app.jobs.notify import notify_admins
from app.jobs.runner import tracked


def reminder_text(base_url: str) -> str:
    url = base_url.rstrip("/") + "/admin/fyers/connect"
    return (
        f'🔑 <a href="{url}">Connect Fyers for today</a>: tap, then enter the OTP and your PIN. '
        "Until then prices come from Yahoo."
    )


@tracked("fyers_reminder")
def fyers_reminder() -> dict:
    now = now_ist()
    if not fyers_client().configured:
        return {"skipped": "Fyers not configured"}
    if now.weekday() >= 5 or now.date() in market_holidays():
        return {"skipped": "not a trading day"}
    if TokenStore(get_db).access_token(now):
        return {"skipped": "already connected"}
    notify_admins(reminder_text(get_settings().app_base_url))
    return {"sent": True}
