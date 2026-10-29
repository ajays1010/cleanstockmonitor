"""Worker entry point: runs every scheduled job. Start with `python -m app.worker`.

Times are IST. Exactly one worker may run per database.
"""

import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.core.timeutil import IST
from app.jobs import market
from app.jobs.fyers_reminder import fyers_reminder
from app.jobs.heartbeat import heartbeat
from app.jobs.scrips import sync_scrips
from app.jobs.stats import update_scrip_stats
from app.jobs.watchdog import bse_watchdog

log = logging.getLogger(__name__)


def build_scheduler() -> BlockingScheduler:
    # max_instances=1 and coalesce: a slow run is never overlapped, and missed runs collapse into one.
    scheduler = BlockingScheduler(
        timezone=IST,
        job_defaults={"max_instances": 1, "coalesce": True, "misfire_grace_time": 60},
    )
    scheduler.add_job(heartbeat, "interval", minutes=5, id="heartbeat")
    settings = get_settings()
    # Filings arrive from early morning to late evening, weekends included.
    if settings.announcement_source == "nse":
        scheduler.add_job(
            market.poll_nse_announcements, "cron", minute="*/3", hour="7-23", id="poll_nse_announcements"
        )
    if settings.bse_fetch == "remote":
        # BSE blocks this server: a fetcher elsewhere reads it, and this checks on it.
        scheduler.add_job(bse_watchdog, "interval", minutes=5, id="bse_watchdog")
    else:
        if settings.announcement_source == "bse":
            scheduler.add_job(
                market.poll_announcements, "cron", minute="*/3", hour="7-23", id="poll_announcements"
            )
        # New listings appear in the exchange lists the evening before or the morning of listing.
        scheduler.add_job(sync_scrips, "cron", hour="6,20", minute=30, id="sync_scrips")
    # The job itself checks trading days and the 09:15-15:40 window.
    scheduler.add_job(
        market.poll_prices, "cron", minute="*/5", hour="9-15", day_of_week="mon-fri", id="poll_prices"
    )
    scheduler.add_job(market.fan_out, "interval", seconds=30, id="fan_out")
    scheduler.add_job(market.retry_failed, "interval", minutes=2, id="retry_failed")
    scheduler.add_job(
        market.daily_summary, "cron", hour=16, minute=0, day_of_week="mon-fri", id="daily_summary"
    )
    scheduler.add_job(market.cleanup, "cron", hour=2, minute=0, id="cleanup")
    # 20-day average volume and DMAs for volume and DMA alerts: after the close, and again
    # before the open to catch scrips added overnight.
    scheduler.add_job(update_scrip_stats, "cron", hour="8,18", minute=30, id="update_scrip_stats")
    # Fyers sessions end at 06:30 IST and can no longer be refreshed (SEBI rules): nudge
    # the admin to log in before the open; Yahoo covers any day without a login.
    scheduler.add_job(fyers_reminder, "cron", hour=8, minute=45, day_of_week="mon-fri", id="fyers_reminder")
    return scheduler


def main() -> None:
    setup_logging()
    get_settings().check_production()
    scheduler = build_scheduler()
    log.info("Worker starting with jobs: %s", ", ".join(job.id for job in scheduler.get_jobs()))
    scheduler.start()


if __name__ == "__main__":
    main()
