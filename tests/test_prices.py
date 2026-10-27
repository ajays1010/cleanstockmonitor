from datetime import datetime
from decimal import Decimal

from app.core.prices import PriceView, price_view
from app.core.timeutil import IST

SNAP = {"ltp": 3795.2, "prev_close": 3348.8, "quoted_at": "2026-10-27T15:25:00+05:30"}


def test_price_view_during_session():
    v = price_view(SNAP, datetime(2026, 10, 27, 15, 27, tzinfo=IST))
    assert v.ltp == Decimal("3795.2")
    assert round(v.change_pct, 2) == Decimal("13.33")
    assert v.label == "as of 15:25"


def test_price_view_after_close_and_next_day():
    assert price_view(SNAP, datetime(2026, 10, 27, 18, 0, tzinfo=IST)).label == "Close, 27 Oct"
    assert price_view(SNAP, datetime(2026, 10, 28, 10, 0, tzinfo=IST)).label == "Close, 27 Oct"


def test_price_view_without_prev_close():
    assert price_view({**SNAP, "prev_close": None}, datetime(2026, 10, 27, 18, tzinfo=IST)).change_pct is None


def test_cards_show_price_and_badge(signed_in, prices):
    signed_in.post("/watchlist/add", data={"scrip_id": "1"})
    signed_in.post("/watchlist/add", data={"scrip_id": "2"})
    prices.views_by_scrip = {
        1: PriceView(Decimal("1450.5"), Decimal("-2.1"), "as of 15:25"),
        2: PriceView(Decimal("3795.2"), Decimal("13.333"), "Close, 27 Oct"),
    }
    text = signed_in.get("/dashboard").text
    assert "₹1,450.50" in text and "🔴 −2.10%" in text and "as of 15:25" in text
    assert "₹3,795.20" in text and "🟢 +13.33%" in text and "Close, 27 Oct" in text


def test_card_without_price(signed_in):
    signed_in.post("/watchlist/add", data={"scrip_id": "3"})
    assert "No price yet" in signed_in.get("/dashboard").text


def test_adding_fetches_one_quote(signed_in, prices):
    signed_in.post("/watchlist/add", data={"scrip_id": "2"})
    assert prices.refreshed == [2]


def test_failed_add_does_not_fetch(signed_in, prices):
    signed_in.post("/watchlist/add", data={"scrip_id": "999"})
    assert prices.refreshed == []


def test_price_lines_endpoint_swaps_only_price_lines(signed_in, prices, watchlist):
    signed_in.post("/watchlist/add", data={"scrip_id": "1"})
    item_id = next(iter(watchlist.rows))
    prices.views_by_scrip = {1: PriceView(Decimal("100"), Decimal("1"), "as of 10:00")}
    text = signed_in.get("/watchlist/prices").text
    assert f'id="price-{item_id}" hx-swap-oob="true"' in text
    assert 'id="price-poller" hx-swap-oob="true"' in text
    assert "Alert settings" not in text  # forms being edited are left alone
