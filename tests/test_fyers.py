import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.core import quote_source
from app.core.timeutil import IST
from app.jobs.scrips import with_fyers_symbols
from app.sources.fyers import FyersClient, FyersError, next_expiry, parse_quotes, parse_symbol_master
from app.sources.quotes import Quote
from app.sources.scrip_master import ScripRow

NOW = datetime(2026, 10, 27, 10, 0, tzinfo=IST)


def client(handler):
    return FyersClient(
        "ABC123-100",
        "secret",
        "https://app.test/admin/fyers/callback",
        httpx.Client(transport=httpx.MockTransport(handler)),
    )


def test_next_expiry_is_next_0630_ist():
    assert next_expiry(datetime(2026, 10, 27, 10, 0, tzinfo=IST)) == datetime(2026, 10, 28, 6, 30, tzinfo=IST)
    assert next_expiry(datetime(2026, 10, 27, 5, 0, tzinfo=IST)) == datetime(2026, 10, 27, 6, 30, tzinfo=IST)


def test_login_url():
    url = urlparse(client(lambda r: httpx.Response(500)).login_url("st8"))
    q = parse_qs(url.query)
    assert url.geturl().startswith("https://api-t1.fyers.in/api/v3/generate-authcode?")
    assert q == {
        "client_id": ["ABC123-100"],
        "redirect_uri": ["https://app.test/admin/fyers/callback"],
        "response_type": ["code"],
        "state": ["st8"],
    }


def test_exchange_code_sends_hash_and_returns_tokens():
    seen = {}

    def handler(req):
        seen.update(json.loads(req.content), url=str(req.url))
        return httpx.Response(200, json={"s": "ok", "access_token": "acc", "refresh_token": "ref"})

    t = client(handler).exchange_code("the-code", NOW)
    assert seen["url"] == "https://api-t1.fyers.in/api/v3/validate-authcode"
    assert seen["grant_type"] == "authorization_code" and seen["code"] == "the-code"
    assert seen["appIdHash"] == hashlib.sha256(b"ABC123-100:secret").hexdigest()
    assert t.access_token == "acc"  # the refresh token Fyers also sends is not kept
    assert t.access_expires_at == datetime(2026, 10, 28, 6, 30, tzinfo=IST)


def test_errors_raise():
    reply = httpx.Response(400, json={"s": "error", "code": -413, "message": "invalid auth code"})
    with pytest.raises(FyersError, match="-413 invalid auth code"):
        client(lambda r: reply).exchange_code("bad", NOW)


# ---- morning reminder


@pytest.fixture
def reminder(monkeypatch):
    from app.jobs import fyers_reminder as fr

    sent = []
    state = {"now": datetime(2026, 10, 29, 8, 45, tzinfo=IST), "token": None, "holidays": set()}
    monkeypatch.setattr(fr, "now_ist", lambda: state["now"])
    monkeypatch.setattr(fr, "fyers_client", lambda: FakeFyers())
    monkeypatch.setattr(fr, "TokenStore", lambda _: FakeTokens(state["token"]))
    monkeypatch.setattr(fr, "market_holidays", lambda: state["holidays"])
    monkeypatch.setattr(fr, "notify_admins", sent.append)
    monkeypatch.setattr(fr, "get_db", lambda: None)
    monkeypatch.setattr("app.jobs.runner.get_db", MagicMock)  # job_runs bookkeeping
    return fr, state, sent


def test_reminder_sent_on_trading_days_without_a_login(reminder):
    fr, state, sent = reminder
    fr.fyers_reminder()
    assert len(sent) == 1 and "/admin/fyers/connect" in sent[0] and "Yahoo" in sent[0]


def test_no_reminder_when_connected_weekend_or_holiday(reminder):
    fr, state, sent = reminder
    state["token"] = "tok"
    fr.fyers_reminder()
    state["token"], state["now"] = None, datetime(2026, 10, 31, 8, 45, tzinfo=IST)  # Saturday
    fr.fyers_reminder()
    state["now"] = datetime(2026, 11, 9, 8, 45, tzinfo=IST)
    state["holidays"] = {state["now"].date()}
    fr.fyers_reminder()
    assert sent == []


SAMPLE = {
    "s": "ok",
    "code": 200,
    "d": [
        {
            "n": "NSE:SBIN-EQ",
            "s": "ok",
            "v": {
                "lp": 590.5,
                "prev_close_price": 598.45,
                "high_price": 600.85,
                "low_price": 585,
                "volume": 27774877,
                "tt": "1793093700",
            },
        },
        {"n": "NSE:BAD-EQ", "s": "error", "v": {"errmsg": "invalid symbol"}},
    ],
}


def test_parse_quotes():
    q = parse_quotes(SAMPLE, NOW)
    assert list(q) == ["NSE:SBIN-EQ"]
    s = q["NSE:SBIN-EQ"]
    assert (s.ltp, s.prev_close, s.day_high, s.day_low, s.source) == (
        Decimal("590.5"),
        Decimal("598.45"),
        Decimal("600.85"),
        Decimal("585"),
        "fyers",
    )


def test_quote_time_comes_from_trading_day_not_tt():
    # tt is midnight UTC of the trading day (05:30 IST); real responses look like this.
    def at(tt):
        data = {"d": [{"n": "X", "s": "ok", "v": {"lp": 1, "tt": str(tt)}}]}
        return parse_quotes(data, NOW)["X"].quoted_at

    today = int(datetime(2026, 10, 27, tzinfo=UTC).timestamp())
    assert at(today) == NOW
    assert at(today - 86400) == datetime(2026, 10, 26, 15, 30, tzinfo=IST)
    assert at("junk") == NOW


def test_quotes_batches_of_50_with_auth_header():
    calls = []

    def handler(req):
        calls.append((req.url.params["symbols"].count(",") + 1, req.headers["Authorization"]))
        return httpx.Response(200, json={"s": "ok", "d": []})

    client(handler).quotes([f"NSE:S{i}-EQ" for i in range(120)], "tok")
    assert calls == [(50, "ABC123-100:tok"), (50, "ABC123-100:tok"), (20, "ABC123-100:tok")]


NSE_CSV = (
    "1,ACUTAAS CHEMICALS LIMITED,0,1,0.1,INE00FF01025,s,d,,NSE:ACUTAAS-EQ,10,10,5578,ACUTAAS\n"
    "2,RAVITA ENGINEERING,0,1200,0.05,INE1UEL01023,s,d,,NSE:RAVITA-ST,10,10,766763,RAVITA\n"
    "3,GOI BOND,0,1,0.01,IN0020200001,s,d,,NSE:GS2030-GS,10,10,9,GS2030\n"
)
BSE_CSV = (
    "4,ACUTAAS CHEMICALS LIMITED,0,1,0.05,INE00FF01025,s,d,,BSE:ACUTAAS-A,12,10,543349,ACUTAAS\n"
    "5,BSE ONLY LTD,0,1,0.05,INE0BSE01011,s,d,,BSE:BSEONLY-X,12,10,530001,BSEONLY\n"
)


def test_symbol_master_and_mapping():
    nse, bse = parse_symbol_master(NSE_CSV, BSE_CSV)
    assert nse == {"INE00FF01025": "NSE:ACUTAAS-EQ", "INE1UEL01023": "NSE:RAVITA-ST"}  # bonds skipped
    assert bse["530001"] == "BSE:BSEONLY-X"
    rows = [
        ScripRow("543349", "ACUTAAS", "Acutaas", "INE00FF01025", "ACUTAAS", "ACUTAAS.NS", None),
        ScripRow("530001", "BSEONLY", "Bse Only", "INE0BSE01011", None, "BSEONLY.BO", None),
        ScripRow("530002", "GONE", "Not in Fyers", None, None, None, None),
    ]
    assert [r.fyers_symbol for r in with_fyers_symbols(rows, nse, bse)] == [
        "NSE:ACUTAAS-EQ",
        "BSE:BSEONLY-X",
        None,
    ]


def _quote(source):
    return Quote(Decimal("10"), Decimal("9"), None, None, None, NOW, source)


class FakeFyers:
    configured = True

    def __init__(self, result=None, error=None):
        self.result, self.error = result or {}, error

    def quotes(self, symbols, token):
        if self.error:
            raise self.error
        return {s: q for s, q in self.result.items() if s in symbols}


class FakeTokens:
    def __init__(self, token):
        self.token = token

    def access_token(self, now):
        return self.token


SCRIPS = [
    {"id": 1, "fyers_symbol": "NSE:A-EQ", "yahoo_symbol": "A.NS"},
    {"id": 2, "fyers_symbol": None, "yahoo_symbol": "B.BO"},
]


@pytest.fixture
def wire(monkeypatch):
    yahoo_calls = []

    def setup(fyers, token):
        monkeypatch.setattr(quote_source, "fyers_client", lambda: fyers)
        monkeypatch.setattr(quote_source, "TokenStore", lambda _: FakeTokens(token))
        monkeypatch.setattr(quote_source, "get_db", lambda: None)
        monkeypatch.setattr(
            quote_source, "fetch_yahoo", lambda sym, http: yahoo_calls.append(sym) or _quote("yahoo")
        )
        return yahoo_calls

    return setup


def test_fyers_first_then_yahoo_for_the_rest(wire):
    yahoo = wire(FakeFyers({"NSE:A-EQ": _quote("fyers")}), "tok")
    out = quote_source.fetch_quotes(SCRIPS)
    assert (out[1].source, out[2].source) == ("fyers", "yahoo")
    assert yahoo == ["B.BO"]


def test_yahoo_when_fyers_not_connected_or_failing(wire):
    yahoo = wire(FakeFyers({"NSE:A-EQ": _quote("fyers")}), None)
    assert {q.source for q in quote_source.fetch_quotes(SCRIPS).values()} == {"yahoo"}
    assert yahoo == ["A.NS", "B.BO"]
    yahoo.clear()
    wire(FakeFyers(error=FyersError("down")), "tok")
    assert {q.source for q in quote_source.fetch_quotes(SCRIPS).values()} == {"yahoo"}


class FakeTokenStore:
    def __init__(self):
        self.saved = None

    def get(self):
        return self.saved

    def save(self, tokens, now):
        self.saved = tokens


@pytest.fixture
def admin_client(signed_in, fakes):
    from app.core.profiles import Profile
    from app.web.deps import get_fyers, get_job_runs, get_tokens

    _, profiles = fakes
    uid = next(iter(profiles.rows))
    profiles.rows[uid] = Profile(uid, "a@example.com", None, is_admin=True)
    store = FakeTokenStore()

    def handler(req):
        body = json.loads(req.content)
        if body.get("code") == "good":
            return httpx.Response(200, json={"s": "ok", "access_token": "acc", "refresh_token": "ref"})
        return httpx.Response(400, json={"s": "error", "code": -413, "message": "invalid auth code"})

    signed_in.app.dependency_overrides[get_fyers] = lambda: client(handler)
    signed_in.app.dependency_overrides[get_tokens] = lambda: store
    signed_in.app.dependency_overrides[get_job_runs] = lambda: lambda: []
    return signed_in, store


def test_admin_page_needs_admin(signed_in):
    assert signed_in.get("/admin").status_code == 403


def test_fyers_login_round_trip(admin_client):
    c, store = admin_client
    assert "Connect Fyers" in c.get("/admin").text
    start = c.get("/admin/fyers/connect")
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
    resp = c.get(
        "/admin/fyers/callback", params={"auth_code": "good", "state": state, "code": "200", "s": "ok"}
    )
    assert resp.status_code == 303 and store.saved.access_token == "acc"


def test_fyers_callback_rejects_wrong_state(admin_client):
    c, store = admin_client
    c.get("/admin/fyers/connect")
    resp = c.get("/admin/fyers/callback", params={"auth_code": "good", "state": "forged"})
    assert resp.status_code == 400 and store.saved is None


def test_fyers_callback_shows_refusal(admin_client):
    c, store = admin_client
    start = c.get("/admin/fyers/connect")
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
    resp = c.get("/admin/fyers/callback", params={"auth_code": "bad", "state": state})
    assert resp.status_code == 400 and "invalid auth code" in resp.text
