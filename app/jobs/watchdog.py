"""With BSE_FETCH=remote, BSE is read by a fetcher elsewhere (app/fetcher.py). This job
warns admins on Telegram when announcement checks stop (BSE ones from the fetcher, or
NSE ones from this worker, per ANNOUNCEMENT_SOURCE) or the scrip list goes stale, and
again when they recover."""

from datetime import datetime, time, timedelta

from app.core.config import get_settings
from app.core.timeutil import IST, now_ist
from app.jobs.fyers_refresh import notify_admins
from app.jobs.runner import last_success, tracked

ANNOUNCEMENTS_STALE = timedelta(minutes=15)
# Two syncs a day when the fetcher runs; new listings matter less than filings, so this
# only warns after several missed days.
SCRIPS_STALE = timedelta(days=3)
# Announcements are fetched 07:00-23:59; give the first run of the day time to happen.
WATCH_FROM = time(7, 15)

_warned: set[str] = set()  # kept in memory: a worker restart may repeat one warning


def _since(last: datetime | None) -> str:
    return f"since {last.astimezone(IST):%d %b %H:%M} IST" if last else "yet"


def checks(
    now: datetime, announcements: datetime | None, scrips: datetime | None, source: str = "BSE"
) -> dict[str, str | None]:
    """A warning per check that fails, None per check that passes. Announcements are not
    judged overnight, so a warning raised in the evening stands until morning."""
    out: dict[str, str | None] = {}
    if now.timetz().replace(tzinfo=None) >= WATCH_FROM:
        stale = announcements is None or now - announcements > ANNOUNCEMENTS_STALE
        out["announcements"] = (
            f"⚠️ No successful {source} announcement check {_since(announcements)}. Missed "
            "filings from the last 2 days are picked up once checks work again."
            if stale
            else None
        )
    stale = scrips is None or now - scrips > SCRIPS_STALE
    out["scrips"] = (
        f"⚠️ The scrip list has not refreshed {_since(scrips)}, so new listings are missing."
        if stale
        else None
    )
    return out


@tracked("bse_watchdog")
def bse_watchdog() -> dict:
    source = get_settings().announcement_source
    job = "poll_nse_announcements" if source == "nse" else "poll_announcements"
    found = checks(now_ist(), last_success(job), last_success("sync_scrips"), source=source.upper())
    for key, warning in found.items():
        if warning and key not in _warned:
            notify_admins(warning)
            _warned.add(key)
        elif warning is None and key in _warned:
            label = f"{source.upper()} announcements" if key == "announcements" else "scrip list"
            notify_admins(f"✅ {label} working again.")
            _warned.discard(key)
    return {"failing": sorted(k for k, w in found.items() if w)}
