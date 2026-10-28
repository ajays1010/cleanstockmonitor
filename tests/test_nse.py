from datetime import datetime

import httpx

from app.core import alerts
from app.core.timeutil import IST
from app.sources import nse

ROWS = [
    {
        "seq_id": "104588231",
        "symbol": "ACUTAAS",
        "sm_name": "Acutaas Chemicals Limited",
        "desc": "Outcome of Board Meeting",
        "attchmntText": "Acutaas Chemicals Limited has submitted to the Exchange, the financial results "
        "for the period ended September 30, 2026.",
        "an_dt": "27-Oct-2026 12:50:09",
        "attchmntFile": "https://nsearchives.nseindia.com/corporate/AMIORG_27102026124848_FR.pdf",
    },
    {
        "seq_id": "104588999",
        "sm_name": "Acutaas Chemicals Limited",
        "desc": "Bagging/Receiving of orders/contracts",
        "attchmntText": "  Acutaas has received   an order  ",
        "an_dt": "28-Oct-2026 09:01:00",
        "attchmntFile": "-",
    },
    {"seq_id": "", "desc": "skipped without an id"},
]


def test_parse_rows():
    first, second = nse.parse_rows(ROWS, "543349")
    assert (first.news_id, first.bse_code, first.company) == (
        "104588231",
        "543349",
        "Acutaas Chemicals Limited",
    )
    assert first.category == "results" and first.bse_category == "NSE"
    assert first.bse_subcategory == "Outcome of Board Meeting"
    assert first.published_at == datetime(2026, 10, 27, 12, 50, 9, tzinfo=IST)
    assert first.pdf_url.endswith("_FR.pdf")
    assert second.category == "orders" and second.pdf_url is None
    assert second.headline == "Acutaas has received an order"


def test_classify_nse_subjects():
    def cat(desc, text=""):
        return nse.parse_rows([{"seq_id": "1", "desc": desc, "attchmntText": text, "an_dt": ""}], "1")[
            0
        ].category

    assert cat("Credit Rating") == "rating"
    assert cat("Analysts/Institutional Investor Meet/Con. Call Updates") == "investor_meet"
    assert cat("Record Date", "Record date for interim dividend") == "corporate_action"
    assert cat("Trading Window") == "other"


def test_fetch_announcements_params_and_errors():
    seen = {}

    def handler(req):
        seen.update(req.url.params)
        if req.url.params["symbol"] == "DOWN":
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json=ROWS[:1])

    http = httpx.Client(transport=httpx.MockTransport(handler))
    [a] = nse.fetch_announcements("ACUTAAS", "543349", http, days=2)
    assert seen["index"] == "equities" and seen["symbol"] == "ACUTAAS" and a.news_id == "104588231"
    assert seen["from_date"] < "99" and len(seen["to_date"]) == 10
    nse.fetch_announcements("ACUTAAS", "543349", http, sme=True)
    assert seen["index"] == "sme"
    try:
        nse.fetch_announcements("DOWN", "1", http)
    except nse.NseError as exc:
        assert "HTTP 503" in str(exc)
    else:
        raise AssertionError("expected NseError")


def test_sme_detection():
    assert nse.is_sme("NSE:RAVITA-ST") and nse.is_sme("NSE:ABC-SM")
    assert not nse.is_sme("NSE:ACUTAAS-EQ") and not nse.is_sme(None)


def test_nse_events_are_tagged_with_their_source():
    [a, _] = nse.parse_rows(ROWS, "543349")
    event = alerts.announcement_event(a, 7, "ACUTAAS", source="nse")
    assert (event["source"], event["source_id"]) == ("nse", "104588231")
    assert "Financial results</i> · NSE · Outcome of Board Meeting" in alerts.render(event)
