"""Fyers API v3: login, token refresh, batched quotes, and the public symbol master.

Login is a browser redirect: the admin signs in at Fyers, Fyers sends an auth code to our
callback, and we exchange it for an access token (valid until 06:30 IST the next day)
and a refresh token. Each morning the refresh token plus the account PIN mints a new
access token, so the admin only logs in again when the refresh token lapses.

Hosts and fields follow the v3 docs and the official fyers-apiv3 SDK (3.2.0). The
refresh call is not in either; it follows Fyers' published refresh flow and falls back
to asking the admin to log in again if Fyers rejects it.
"""

import csv
import hashlib
import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlencode

import httpx

from app.core.timeutil import IST, MARKET_CLOSE
from app.sources.quotes import Quote

log = logging.getLogger(__name__)

API = "https://api-t1.fyers.in/api/v3"
DATA = "https://api-t1.fyers.in/data"
SYMBOL_FILES = {
    "NSE": "https://public.fyers.in/sym_details/NSE_CM.csv",
    "BSE": "https://public.fyers.in/sym_details/BSE_CM.csv",
}
MAX_SYMBOLS_PER_CALL = 50  # Standard plan
TOKEN_EXPIRY_TIME = (6, 30)  # access tokens expire daily at 06:30 IST
REFRESH_TOKEN_LIFETIME = timedelta(days=15)
# NSE equity series in the symbol master; the rest are bonds, G-secs, MFs and so on.
NSE_EQUITY_SERIES = {"EQ", "BE", "BZ", "SM", "ST"}


class FyersError(Exception):
    pass


@dataclass(frozen=True)
class Tokens:
    access_token: str
    refresh_token: str | None
    access_expires_at: datetime
    refresh_expires_at: datetime | None


def app_id_hash(app_id: str, secret: str) -> str:
    return hashlib.sha256(f"{app_id}:{secret}".encode()).hexdigest()


def next_expiry(now: datetime) -> datetime:
    """The next 06:30 IST after `now`."""
    local = now.astimezone(IST)
    cut = local.replace(hour=TOKEN_EXPIRY_TIME[0], minute=TOKEN_EXPIRY_TIME[1], second=0, microsecond=0)
    return cut if local < cut else cut + timedelta(days=1)


class FyersClient:
    def __init__(self, app_id: str, secret: str, redirect_uri: str, http: httpx.Client | None = None):
        self.app_id = app_id
        self.secret = secret
        self.redirect_uri = redirect_uri
        self.http = http or httpx.Client(timeout=20)

    @property
    def configured(self) -> bool:
        return bool(self.app_id and self.secret)

    def login_url(self, state: str) -> str:
        query = urlencode(
            {
                "client_id": self.app_id,
                "redirect_uri": self.redirect_uri,
                "response_type": "code",
                "state": state,
            }
        )
        return f"{API}/generate-authcode?{query}"

    def exchange_code(self, auth_code: str, now: datetime) -> Tokens:
        data = self._post(
            "/validate-authcode",
            {
                "grant_type": "authorization_code",
                "appIdHash": app_id_hash(self.app_id, self.secret),
                "code": auth_code,
            },
        )
        return Tokens(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            access_expires_at=next_expiry(now),
            refresh_expires_at=now + REFRESH_TOKEN_LIFETIME if data.get("refresh_token") else None,
        )

    def refresh(self, refresh_token: str, pin: str, now: datetime) -> str:
        """A new access token from the refresh token and the account PIN."""
        data = self._post(
            "/validate-refresh-token",
            {
                "grant_type": "refresh_token",
                "appIdHash": app_id_hash(self.app_id, self.secret),
                "refresh_token": refresh_token,
                "pin": pin,
            },
        )
        return data["access_token"]

    def quotes(self, symbols: list[str], access_token: str) -> dict[str, Quote]:
        out: dict[str, Quote] = {}
        for i in range(0, len(symbols), MAX_SYMBOLS_PER_CALL):
            batch = symbols[i : i + MAX_SYMBOLS_PER_CALL]
            try:
                resp = self.http.get(
                    f"{DATA}/quotes",
                    params={"symbols": ",".join(batch)},  # httpx URL-encodes M&M as M%26M
                    headers={"Authorization": f"{self.app_id}:{access_token}"},
                )
                data = resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                raise FyersError(f"quotes: {exc}") from exc
            if data.get("s") != "ok":
                raise FyersError(f"quotes: {data.get('code')} {data.get('message')}")
            out.update(parse_quotes(data))
        return out

    def _post(self, path: str, body: dict) -> dict:
        try:
            resp = self.http.post(API + path, json=body)
            data = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise FyersError(f"{path}: {exc}") from exc
        if data.get("s") != "ok" or "access_token" not in data:
            raise FyersError(f"{path}: {data.get('code')} {data.get('message')}")
        return data


def _dec(value) -> Decimal | None:
    return None if value in (None, "") else Decimal(str(value))


def _quoted_at(tt, now: datetime) -> datetime:
    """Fyers' tt is the trading day (midnight UTC), not the trade time. Today's quote is
    as of now; an older one is that day's close, so stale quotes stay detectable."""
    try:
        day = datetime.fromtimestamp(int(tt), tz=UTC).date()
    except (TypeError, ValueError):
        return now
    if day >= now.date():
        return now
    return datetime.combine(day, MARKET_CLOSE, tzinfo=IST)


def parse_quotes(data: dict, now: datetime | None = None) -> dict[str, Quote]:
    now = (now or datetime.now(IST)).astimezone(IST)
    out = {}
    for item in data.get("d") or []:
        v = item.get("v") or {}
        if item.get("s") != "ok" or v.get("lp") in (None, ""):
            continue
        quoted_at = _quoted_at(v.get("tt"), now)
        out[item["n"]] = Quote(
            ltp=Decimal(str(v["lp"])),
            prev_close=_dec(v.get("prev_close_price")),
            day_high=_dec(v.get("high_price")),
            day_low=_dec(v.get("low_price")),
            volume=v.get("volume"),
            quoted_at=quoted_at,
            source="fyers",
        )
    return out


def parse_symbol_master(nse_csv: str, bse_csv: str) -> tuple[dict[str, str], dict[str, str]]:
    """(ISIN -> NSE ticker, BSE scrip code -> BSE ticker) from Fyers' symbol files.

    Columns: 5 = ISIN, 9 = ticker (NSE:SBIN-EQ), 12 = exchange token (the BSE scrip code
    in the BSE file).
    """
    nse: dict[str, str] = {}
    for row in csv.reader(io.StringIO(nse_csv)):
        if len(row) > 12 and row[9].rsplit("-", 1)[-1] in NSE_EQUITY_SERIES and row[5]:
            nse.setdefault(row[5], row[9])
    bse: dict[str, str] = {}
    for row in csv.reader(io.StringIO(bse_csv)):
        if len(row) > 12 and row[9].startswith("BSE:") and row[12]:
            bse.setdefault(row[12], row[9])
    return nse, bse


def fetch_symbol_master(client: httpx.Client) -> tuple[dict[str, str], dict[str, str]]:
    texts = {}
    for exchange, url in SYMBOL_FILES.items():
        resp = client.get(url, timeout=90)
        resp.raise_for_status()
        texts[exchange] = resp.text
    return parse_symbol_master(texts["NSE"], texts["BSE"])
