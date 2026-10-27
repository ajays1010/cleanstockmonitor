from datetime import date, datetime

from app.core.timeutil import IST, is_market_open, is_trading_day


def test_weekend_is_not_a_trading_day():
    assert not is_trading_day(date(2026, 10, 31), set())  # Saturday


def test_holiday_is_not_a_trading_day():
    diwali = date(2026, 11, 9)
    assert not is_trading_day(diwali, {diwali})


def test_market_hours_use_ist_even_for_utc_input():
    # 04:00 UTC on a Tuesday is 09:30 IST: market open.
    utc_moment = datetime.fromisoformat("2026-10-27T04:00:00+00:00")
    assert is_market_open(utc_moment, set())
    # 10:30 UTC is 16:00 IST: closed.
    assert not is_market_open(datetime.fromisoformat("2026-10-27T10:30:00+00:00"), set())


def test_market_open_boundaries():
    day = (2026, 10, 27)
    assert is_market_open(datetime(*day, 9, 15, tzinfo=IST), set())
    assert not is_market_open(datetime(*day, 9, 14, tzinfo=IST), set())
    assert is_market_open(datetime(*day, 15, 30, tzinfo=IST), set())
