"""The signed link in the morning Fyers reminder. Tapping it in Telegram goes straight to
Fyers' login (phone, OTP, PIN) without signing in to this site first.

The link only starts a Fyers login and is good for the day it was sent; whoever uses it
still needs the Fyers account's OTP and PIN, so a leaked link cannot connect anything."""

from datetime import date

from itsdangerous import BadSignature, URLSafeTimedSerializer

from app.core.config import get_settings

SALT = "fyers-connect-link"
MAX_AGE = 18 * 3600  # seconds; also checked against the day below


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(get_settings().session_secret.get_secret_value(), salt=SALT)


def make_token(day: date) -> str:
    return _serializer().dumps({"day": day.isoformat()})


def check_token(token: str, today: date) -> bool:
    try:
        data = _serializer().loads(token, max_age=MAX_AGE)
    except BadSignature:  # also covers SignatureExpired
        return False
    return isinstance(data, dict) and data.get("day") == today.isoformat()
