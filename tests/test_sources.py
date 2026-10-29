from datetime import datetime
from decimal import Decimal

import pytest

from app.core.timeutil import IST
from app.sources.bse import classify, parse_rows
from app.sources.quotes import parse_yahoo


@pytest.mark.parametrize(
    ("cat", "sub", "headline", "expected"),
    [
        ("Result", "Financial Results", "Unaudited Financial Results for Q2", "results"),
        ("Company Update", "Analyst / Investor Meet", "Audio link of the Earnings Call", "routine"),
        ("Company Update", "Investor Presentation", "Investor Presentation Q2 FY27", "investor_meet"),
        ("Company Update", "Credit Rating", "Intimation of Credit Rating by ICRA", "rating"),
        ("Company Update", "Award of Order / Receipt of Order", "Bagging of order worth Rs 500 Cr", "orders"),
        ("Company Update", "General", "Receipt of work order from NTPC", "orders"),
        ("Company Update", "General", "Disclosure of court order of the NCLT", "other"),
        ("Company Update", "General", "Shareholders were informed in order to comply", "other"),
        ("Corp. Action", "Dividend", "Record date for interim dividend", "corporate_action"),
        ("Board Meeting", "Board Meeting Intimation", "Board Meeting on 5 November", "board_meeting"),
        ("AGM/EGM", "Postal Ballot", "Outcome of Postal Ballot", "other"),
        ("Company Update", "General", "Newspaper publication", "routine"),
        (
            "Company Update",
            "Newspaper Publication",
            "Newspaper publication of the financial results",
            "routine",
        ),
        ("Company Update", "General", "Grant of 5,50,000 Employee Stock Options (ESOPs)", "routine"),
        ("Company Update", "ESOP/ESOS/ESPS", "Allotment under ESOS 2023", "routine"),
        (
            "Company Update",
            "Analyst / Investor Meet",
            "Recording to earnings call held on 27 October",
            "routine",
        ),
        ("Company Update", "Analyst / Investor Meet", "Transcript of the earnings call", "investor_meet"),
        (
            "Company Update",
            "Investor Presentation",
            "Investor Presentation on financial results",
            "investor_meet",
        ),
        ("Company Update", "General", "Closure of trading window", "routine"),
        ("Company Update", "Press Release / Media Release", "Press release on financial results", "results"),
        ("Company Update", "General", "Business update for the quarter ended September 30", "results"),
        ("Company Update", "General", "Provisional figures of advances and deposits", "results"),
        ("Company Update", "General", "Board approves fund raising through QIP", "funding"),
        ("Company Update", "General", "Acquisition of 51% stake in XYZ Pvt Ltd", "funding"),
        ("Company Update", "General", "Disclosure under Regulation 29 (Substantial Acquisition)", "other"),
        ("Company Update", "Change in Directorate", "Resignation of independent director", "management"),
        ("Company Update", "General", "Appointment of Mr. A as Chief Financial Officer", "management"),
    ],
)
def test_classify(cat, sub, headline, expected):
    assert classify(cat, sub, headline) == expected


def test_parse_bse_row():
    rows = [
        {
            "NEWSID": "2c93-abc",
            "SCRIP_CD": 533096,
            "SLONGNAME": "Adani Power Ltd",
            "HEADLINE": "Scrutinizer''s  Report",
            "CATEGORYNAME": "AGM/EGM",
            "SUBCATNAME": "Postal Ballot",
            "DissemDT": "2026-10-26T20:11:38.877",
            "ATTACHMENTNAME": "59c8.pdf",
        },
        {"NEWSID": "", "HEADLINE": "skipped: no id"},
    ]
    [a] = parse_rows(rows)
    assert a.headline == "Scrutinizer's Report"
    assert a.bse_code == "533096"
    assert a.published_at == datetime(2026, 10, 26, 20, 11, 38, 877000, tzinfo=IST)
    assert a.pdf_url == "https://www.bseindia.com/xml-data/corpfiling/AttachLive/59c8.pdf"
    assert a.category == "other"


def test_parse_bse_row_without_attachment():
    [a] = parse_rows([{"NEWSID": "x", "SCRIP_CD": 1, "HEADLINE": "h", "DissemDT": "2026-10-26T10:00:00"}])
    assert a.pdf_url is None


def test_parse_yahoo_meta():
    meta = {
        "regularMarketPrice": 188.65,
        "chartPreviousClose": 186.55,
        "regularMarketDayHigh": 189.15,
        "regularMarketDayLow": 185.1,
        "regularMarketVolume": 19189144,
        "regularMarketTime": 1793092624,
    }
    q = parse_yahoo(meta)
    assert q.ltp == Decimal("188.65") and q.prev_close == Decimal("186.55")
    assert round(q.change_pct, 2) == Decimal("1.13")
    assert q.quoted_at.tzinfo == IST


def test_parse_yahoo_without_price():
    assert parse_yahoo({"regularMarketTime": 1}) is None
