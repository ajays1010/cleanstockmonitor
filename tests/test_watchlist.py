from decimal import Decimal

import pytest

from app.core.categories import CATEGORIES
from app.core.watchlist import Scrip, WatchlistError, parse_settings, rank_matches


def test_blank_fields_turn_alerts_off():
    s = parse_settings("", "", "", list(CATEGORIES))
    assert (s.spike_pct, s.price_above, s.price_below) == (None, None, None)


def test_all_categories_are_stored_as_no_filter():
    assert parse_settings("5", "", "", list(CATEGORIES)).categories == []


def test_some_categories_keep_catalogue_order():
    assert parse_settings("5", "", "", ["rating", "results"]).categories == ["results", "rating"]


def test_no_categories_is_refused():
    with pytest.raises(WatchlistError, match="at least one"):
        parse_settings("5", "", "", [])


def test_prices_accept_commas_and_round_to_paise():
    s = parse_settings("2.5", "2,950.456", "2800", list(CATEGORIES))
    assert s.spike_pct == Decimal("2.50")
    assert s.price_above == Decimal("2950.46")
    assert s.price_below == Decimal("2800.00")


@pytest.mark.parametrize(
    ("spike", "above", "below", "message"),
    [
        ("0.1", "", "", "between"),
        ("60", "", "", "between"),
        ("abc", "", "", "must be a number"),
        ("NaN", "", "", "must be a number"),
        ("5", "-1", "", "more than 0"),
        ("5", "100", "100", "lower than"),
    ],
)
def test_invalid_settings(spike, above, below, message):
    with pytest.raises(WatchlistError, match=message):
        parse_settings(spike, above, below, list(CATEGORIES))


def test_unknown_category_is_refused():
    with pytest.raises(WatchlistError, match="Unknown"):
        parse_settings("5", "", "", ["results", "made_up"])


def test_rank_puts_exact_symbol_first():
    scrips = [
        Scrip(1, "500570", "Tata Motors Ltd", "TATAMOTORS"),
        Scrip(2, "500400", "Tata Power Company Ltd", "TATAPOWER"),
        Scrip(3, "532540", "Tata Consultancy Services Ltd", "TCS"),
        Scrip(4, "999999", "Big Tata Holdings", None),
    ]
    # Exact symbol first; the rest by name.
    assert [s.id for s in rank_matches("tcs", scrips, 10)] == [3, 4, 1, 2]
    # Names starting with the query beat names merely containing it.
    assert [s.id for s in rank_matches("tata", scrips, 2)] == [3, 1]


def test_rank_matches_bse_code_exactly():
    scrips = [Scrip(1, "500400", "Tata Power", "TATAPOWER"), Scrip(2, "500401", "Other", None)]
    assert rank_matches("500400", scrips, 10)[0].id == 1
