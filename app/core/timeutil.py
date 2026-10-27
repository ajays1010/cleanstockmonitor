"""India time helpers. Store UTC in the database; use IST for schedules and messages."""

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)


def now_ist() -> datetime:
    return datetime.now(IST)


def is_trading_day(day: date, holidays: set[date]) -> bool:
    return day.weekday() < 5 and day not in holidays


def is_market_open(moment: datetime, holidays: set[date]) -> bool:
    local = moment.astimezone(IST)
    return is_trading_day(local.date(), holidays) and MARKET_OPEN <= local.time() <= MARKET_CLOSE
