"""Worker entry point: runs every scheduled job. Start with `python -m app.worker`."""

import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.core.timeutil import IST
from app.jobs.heartbeat import heartbeat

log = logging.getLogger(__name__)


def build_scheduler() -> BlockingScheduler:
    # max_instances=1 and coalesce: a slow run is never overlapped, and missed runs collapse into one.
    scheduler = BlockingScheduler(
        timezone=IST,
        job_defaults={"max_instances": 1, "coalesce": True, "misfire_grace_time": 60},
    )
    scheduler.add_job(heartbeat, "interval", minutes=5, id="heartbeat")
    return scheduler


def main() -> None:
    setup_logging()
    get_settings().check_production()
    scheduler = build_scheduler()
    log.info("Worker starting with jobs: %s", ", ".join(job.id for job in scheduler.get_jobs()))
    scheduler.start()


if __name__ == "__main__":
    main()
