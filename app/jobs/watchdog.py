"""With BSE_FETCH=remote, BSE is read by the fetcher on Render (app/fetcher.py). This
job warns admins on Telegram when its runs stop, and again when they resume."""

from datetime import datetime, time, timedelta

from app.core.timeutil import IST, now_ist
from app.jobs.fyers_refresh import notify_admins
from app.jobs.runner import last_success, tracked

ANNOUNCEMENTS_STALE = timedelta(minutes=15)
SCRIPS_STALE = timedelta(hours=26)  # two syncs a day; one missed slot is tolerated
# Announcements are fetched 07:00-23:59; give the first run of the day time to happen.
WATCH_FROM = time(7, 15)

LABELS = {"announcements": "BSE announcements", "scrips": "scrip list"}
_warned: set[str] = set()  # kept in memory: a worker restart may repeat one warning


def _since(last: datetime | None) -> str:
    return f"since {last.astimezone(IST):%d %b %H:%M} IST" if last else "yet"


def checks(now: datetime, announcements: datetime | None, scrips: datetime | None) -> dict[str, str | None]:
    """A warning per check that fails, None per check that passes. Announcements are not
    judged overnight, so a warning raised in the evening stands until morning."""
    out: dict[str, str | None] = {}
    if now.timetz().replace(tzinfo=None) >= WATCH_FROM:
        stale = announcements is None or now - announcements > ANNOUNCEMENTS_STALE
        out["announcements"] = (
            f"⚠️ BSE fetcher: no successful announcement check {_since(announcements)}. "
            "Check the Render service and the cron-job.org job; missed filings from the "
            "last 2 days are picked up once it runs again."
            if stale
            else None
        )
    stale = scrips is None or now - scrips > SCRIPS_STALE
    out["scrips"] = (
        f"⚠️ BSE fetcher: the scrip list has not refreshed {_since(scrips)}, so new listings are missing."
        if stale
        else None
    )
    return out


@tracked("bse_watchdog")
def bse_watchdog() -> dict:
    found = checks(now_ist(), last_success("poll_announcements"), last_success("sync_scrips"))
    for key, warning in found.items():
        if warning and key not in _warned:
            notify_admins(warning)
            _warned.add(key)
        elif warning is None and key in _warned:
            notify_admins(f"✅ BSE fetcher: {LABELS[key]} working again.")
            _warned.discard(key)
    return {"failing": sorted(k for k, w in found.items() if w)}
