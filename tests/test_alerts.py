from datetime import date, datetime, timedelta
from decimal import Decimal

from app.core import alerts
from app.core.timeutil import IST
from app.sources.bse import Announcement
from app.sources.quotes import Quote

NOW = datetime(2026, 10, 27, 14, 0, tzinfo=IST)
TODAY = date(2026, 10, 27)
SCRIP = {"id": 7, "name": "Adani Power Ltd", "nse_symbol": "ADANIPOWER", "bse_code": "533096"}


def quote(ltp, prev="100"):
    return Quote(Decimal(ltp), Decimal(prev), Decimal("110"), Decimal("95"), 1000, NOW, "yahoo")


def settings(spike="5", above=None, below=None):
    return {"spike_pct": spike, "price_above": above, "price_below": below}


def test_spike_up_fires_once_per_day_key():
    [e] = alerts.price_events("u1", SCRIP, settings(), quote("105.5"), TODAY)
    assert e["kind"] == "price_spike"
    assert e["source_id"] == "spike:u1:7:2026-10-27:up"
    assert e["payload"]["user_id"] == "u1" and e["payload"]["direction"] == "up"


def test_small_move_does_not_fire():
    assert alerts.price_events("u1", SCRIP, settings(), quote("104.9"), TODAY) == []


def test_spike_down_and_levels():
    events = alerts.price_events(
        "u1", SCRIP, settings(spike="3", above="200", below="97"), quote("96"), TODAY
    )
    assert {e["source_id"].split(":")[0] for e in events} == {"spike", "below"}
    assert any(e["source_id"].endswith(":down") for e in events)


def test_spike_off_when_blank():
    assert alerts.price_events("u1", SCRIP, settings(spike=None), quote("150"), TODAY) == []


def announcement(published=NOW - timedelta(minutes=10), category="results"):
    a = Announcement(
        "n1",
        "533096",
        "Adani Power Ltd",
        "Financial Results Q2",
        "Result",
        "Financial Results",
        published,
        "https://x/y.pdf",
        category,
    )
    return alerts.announcement_event(a, 7, "ADANIPOWER")


def watch(user="u1", created=NOW - timedelta(days=1), categories=None):
    return {"user_id": user, "scrip_id": 7, "created_at": created.isoformat(), "categories": categories or []}


def test_announcement_goes_to_watchers_chats_once():
    event = announcement()
    chats = {"u1": [111, -500], "u2": [-500, 222]}  # group -500 shared by both users
    pairs = alerts.targets(event, [watch("u1"), watch("u2")], chats, NOW)
    assert sorted(c for c, _ in pairs) == [-500, 111, 222]


def test_category_filter():
    event = announcement(category="other")
    assert alerts.targets(event, [watch(categories=["results"])], {"u1": [1]}, NOW) == []
    assert alerts.targets(event, [watch(categories=["other"])], {"u1": [1]}, NOW) == [(1, "u1")]


def test_routine_filings_are_never_sent():
    assert alerts.targets(announcement(category="routine"), [watch()], {"u1": [1]}, NOW) == []


def filing(event_id, category="results", sub="Financial Results", title="Results", hours_ago=1, **extra):
    published = (NOW - timedelta(hours=hours_ago)).isoformat()
    payload = {"category": category, "bse_subcategory": sub, **extra}
    return {
        "id": event_id,
        "kind": "announcement",
        "title": title,
        "occurred_at": published,
        "payload": payload,
    }


def test_only_the_first_results_filing_of_the_day_is_sent():
    outcome = filing(1, "board_meeting", "Outcome of Board Meeting", "Outcome of Board Meeting", hours_ago=3)
    results, press = filing(2, hours_ago=2), filing(3, sub="Press Release / Media Release")
    assert alerts.is_results_filing(outcome)
    assert not alerts.repeats_results(outcome, [])
    assert alerts.repeats_results(results, [outcome])
    assert alerts.repeats_results(press, [outcome, results])
    # A skipped repeat does not count as sent, and other filings are never repeats.
    assert not alerts.repeats_results(
        results, [{**outcome, "payload": {**outcome["payload"], "skipped": "x"}}]
    )
    assert not alerts.repeats_results(filing(4, "investor_meet", "Investor Presentation"), [outcome])
    intimation = filing(5, "board_meeting", "Board Meeting Intimation", "Board meeting on 5 Nov")
    assert not alerts.is_results_filing(intimation)


def test_results_on_another_day_are_sent_again():
    yesterday = filing(1, hours_ago=26)
    assert not alerts.repeats_results(filing(2), [yesterday])


def test_old_announcements_and_ones_before_adding_are_skipped():
    old = announcement(published=NOW - timedelta(hours=30))
    assert alerts.targets(old, [watch()], {"u1": [1]}, NOW) == []
    before_add = announcement(published=NOW - timedelta(hours=2))
    assert alerts.targets(before_add, [watch(created=NOW - timedelta(hours=1))], {"u1": [1]}, NOW) == []


def test_price_event_targets_only_its_user():
    [event] = alerts.price_events("u2", SCRIP, settings(), quote("110"), TODAY)
    assert alerts.targets(event, [watch("u1"), watch("u2")], {"u1": [1], "u2": [2]}, NOW) == [(2, "u2")]


def test_render_announcement_escapes_and_links_pdf():
    event = announcement()
    event["title"] = "Results <Q2> & more"
    text = alerts.render(event)
    assert "<b>Adani Power Ltd</b> (ADANIPOWER)" in text
    assert "Results &lt;Q2&gt; &amp; more" in text
    assert '<a href="https://x/y.pdf">Open filing (PDF)</a>' in text
    assert "27 Oct 2026, 13:50 IST" in text


def test_render_price_spike_up():
    [event] = alerts.price_events("u1", SCRIP, settings(), quote("106"), TODAY)
    assert alerts.render(event) == (
        "📈 Price Alert: <b>Adani Power Ltd</b> (ADANIPOWER)\n"
        "Price: ₹106.00 (🟢 +6.00%) vs prev close ₹100.00\n"
        "Day (High–Low): ₹110.00 – ₹95.00"
    )


def test_render_price_spike_down_is_red():
    [event] = alerts.price_events("u1", SCRIP, settings(), quote("94"), TODAY)
    text = alerts.render(event)
    assert text.startswith("📉 Price Alert:")
    assert "(🔴 −6.00%)" in text


def test_render_price_level():
    [event] = alerts.price_events(
        "u1", SCRIP, settings(spike=None, above="2500"), quote("2600", "2590"), TODAY
    )
    text = alerts.render(event)
    assert text.startswith("🔔 Price Alert: <b>Adani Power Ltd</b> (ADANIPOWER)\nCrossed above ₹2,500.00\n")
    assert "Price: ₹2,600.00 (🟢 +0.39%) vs prev close ₹2,590.00" in text


def test_change_badge():
    from decimal import Decimal as D

    assert alerts.change_badge(D("13.333")) == "🟢 +13.33%"
    assert alerts.change_badge(D("-2.1")) == "🔴 −2.10%"
    assert alerts.change_badge(D("0")) == "⚪ 0.00%"


def test_summary_text():
    rows = [
        {**SCRIP, "quote": {"ltp": 188.65, "prev_close": 186.55}},
        {
            "id": 8,
            "name": "Manorama Industries Ltd",
            "nse_symbol": "MANORAMA",
            "bse_code": "541974",
            "quote": None,
        },
    ]
    text = alerts.summary_text(rows, {7: 2}, TODAY)
    assert "Close summary, 27 Oct 2026" in text
    assert "ADANIPOWER: ₹188.65 (🟢 +1.13%) · 2 filings" in text
    assert "MANORAMA: no price today" in text
