from datetime import date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app import worker
from app.core import alerts
from app.core.prices import price_view
from app.core.scrip_stats import completed, compute
from app.core.timeutil import IST
from app.core.watchlist import WatchlistError, parse_settings, parse_shares
from app.sources.quotes import DailyBar, Quote, parse_daily

SCRIP = {"id": 7, "name": "Acutaas Chemicals Ltd", "nse_symbol": "ACUTAAS", "bse_code": "543349"}
TODAY = date(2026, 10, 29)
STATS = {"avg_volume_20": 500_000, "dma_20": "3700", "dma_50": "3420.50", "dma_200": "2980"}


def quote(ltp="3795.20", prev="3600", volume=1_600_000, hour=11, minute=20, high="3800", low="3590"):
    return Quote(
        Decimal(ltp),
        Decimal(prev),
        Decimal(high),
        Decimal(low),
        volume,
        datetime(2026, 10, 29, hour, minute, tzinfo=IST),
        "fyers",
    )


def events(settings, q=None, stats=STATS):
    base = {"spike_pct": None, "price_above": None, "price_below": None}
    return alerts.price_events("u1", SCRIP, {**base, **settings}, q or quote(), TODAY, stats)


# ---- volume multiple


def test_volume_multiple_fires_with_midday_flag():
    [e] = events({"volume_multiple": "3"})
    p = e["payload"]
    assert (p["rule"], p["ratio"], p["early"], p["at"]) == ("volume_x", "3.2", True, "11:20")
    assert e["source_id"] == "volx:u1:7:2026-10-29:3"
    text = alerts.render(e)
    assert "📊 Volume Alert: <b>Acutaas Chemicals Ltd</b> (ACUTAAS)" in text
    assert "Volume: 16.00 L by 11:20 (3.2× 20-day avg of 5.00 L) ⭐ before midday" in text
    assert "Price: ₹3,795.20 (🟢 +5.42%)" in text


def test_volume_multiple_after_midday_and_below_threshold():
    [e] = events({"volume_multiple": "3"}, quote(hour=14, minute=5))
    assert e["payload"]["early"] is False and "before midday" not in alerts.render(e)
    assert events({"volume_multiple": "3"}, quote(volume=1_400_000)) == []  # 2.8×
    assert events({"volume_multiple": "3"}, stats={}) == []  # no history yet


# ---- volume threshold


def test_volume_threshold_and_circuit_hint():
    [e] = events({"volume_above": 1_000_000})
    assert e["source_id"] == "vola:u1:7:2026-10-29:1000000"
    assert "Volume crossed 10.00 L: now 16.00 L by 11:20" in alerts.render(e)
    assert "circuit" not in alerts.render(e)
    locked = quote(ltp="250", prev="238.10", high="250", low="250", volume=120_000)
    [e] = events({"volume_above": 100_000}, locked)
    assert "looks circuit-locked" in alerts.render(e)
    assert events({"volume_above": 2_000_000}) == []


# ---- DMA crossings


def test_dma_cross_up_and_down():
    [e] = events({"dma_alerts": [50]}, quote(ltp="3450", prev="3400"))
    assert (e["payload"]["direction"], e["source_id"]) == ("up", "dma50:u1:7:2026-10-29:up")
    assert "Crossed above its 50 DMA (₹3,420.50)" in alerts.render(e)
    [e] = events({"dma_alerts": [20]}, quote(ltp="3690", prev="3710"))
    assert "Fell below its 20 DMA (₹3,700.00)" in alerts.render(e)


def test_no_dma_alert_without_a_cross_or_history():
    assert events({"dma_alerts": [20, 50, 200]}, quote(ltp="3800", prev="3750")) == []  # above all
    assert events({"dma_alerts": [200]}, quote(ltp="3000", prev="2950"), stats={"dma_200": None}) == []


def test_all_rules_off_by_default():
    assert events({}) == []


def test_volume_text():
    assert [alerts.volume_text(n) for n in (85_400, 184_000, 21_000_000)] == ["85,400", "1.84 L", "2.10 Cr"]


# ---- settings


def test_parse_volume_and_dma_settings():
    s = parse_settings("5", "", "", ["results"], "3", "5L", ["200", "50"])
    assert (s.volume_multiple, s.volume_above, s.dma_alerts) == (Decimal("3.00"), 500_000, [50, 200])
    off = parse_settings("5", "", "", ["results"])
    assert (off.volume_multiple, off.volume_above, off.dma_alerts) == (None, None, [])


@pytest.mark.parametrize(
    ("raw", "shares"),
    [
        ("500000", 500_000),
        ("5,00,000", 500_000),
        ("500K", 500_000),
        ("1.5 lakh", 150_000),
        ("2cr", 20_000_000),
    ],
)
def test_parse_shares(raw, shares):
    assert parse_shares(raw) == shares


@pytest.mark.parametrize(
    ("multiple", "above", "dmas", "message"),
    [
        ("1", "", [], "Volume multiple"),
        ("", "lots", [], "Volume threshold"),
        ("", "5 bananas", [], "Volume threshold"),
        ("", "", ["100"], "Unknown DMA"),
    ],
)
def test_bad_volume_and_dma_settings(multiple, above, dmas, message):
    with pytest.raises(WatchlistError, match=message):
        parse_settings("5", "", "", ["results"], multiple, above, dmas)


# ---- daily history and stats


def test_parse_daily_skips_days_without_close():
    stamps = [int(datetime(2026, 10, d, 9, 15, tzinfo=IST).timestamp()) for d in (27, 28, 29)]
    result = {
        "timestamp": stamps,
        "indicators": {"quote": [{"close": [100.5, None, 102], "volume": [10, 0, 30]}]},
    }
    bars = parse_daily(result)
    assert [(b.day.day, b.close, b.volume) for b in bars] == [
        (27, Decimal("100.5"), 10),
        (29, Decimal("102"), 30),
    ]


def bars(n, start=date(2026, 1, 1)):
    return [DailyBar(start + timedelta(days=i), Decimal(100 + i), 1000 + i) for i in range(n)]


def test_compute_stats_needs_full_windows():
    stats = compute(bars(60))
    assert stats["avg_volume_20"] == round(sum(1000 + i for i in range(40, 60)) / 20)
    assert stats["dma_20"] == "149.50" and stats["dma_50"] == "134.50" and stats["dma_200"] is None
    assert compute(bars(10))["avg_volume_20"] is None
    assert compute([]) is None


def test_running_session_is_left_out_until_the_close():
    history = bars(3, start=TODAY - timedelta(days=2))
    assert completed(history, datetime(2026, 10, 29, 11, 0, tzinfo=IST))[-1].day == TODAY - timedelta(days=1)
    assert completed(history, datetime(2026, 10, 29, 16, 0, tzinfo=IST))[-1].day == TODAY


# ---- dashboard and schedule


def test_price_view_shows_known_dmas():
    snapshot = {"ltp": "3795.20", "prev_close": "3600", "quoted_at": "2026-10-29T11:20:00+05:30"}
    view = price_view(snapshot, datetime(2026, 10, 29, 11, 30, tzinfo=IST), {**STATS, "dma_200": None})
    assert [(n, str(level)) for n, level, _ in view.dmas] == [(20, "3700"), (50, "3420.50")]
    assert round(view.dmas[1][2], 1) == Decimal("11.0")
    assert price_view(snapshot, datetime(2026, 10, 29, 11, 30, tzinfo=IST)).dmas == ()


def test_worker_refreshes_stats():
    settings = SimpleNamespace(announcement_source="bse", bse_fetch="remote")
    original = worker.get_settings
    worker.get_settings = lambda: settings
    try:
        ids = {job.id for job in worker.build_scheduler().get_jobs()}
    finally:
        worker.get_settings = original
    assert "update_scrip_stats" in ids
