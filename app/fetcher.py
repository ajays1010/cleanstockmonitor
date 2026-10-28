"""BSE fetcher: a tiny web app for a host BSE does not block (Render's free plan).

BSE refuses requests from the main server, so this app does only the BSE reads and
writes them straight to the database: announcements on every call, and the scrip list
after 06:30 and 20:30 IST. PDFs, summaries and Telegram stay on the main server, so
memory stays under 100 MB.

An external scheduler (cron-job.org) sends POST /run every 3 minutes with the secret
in the X-Fetcher-Key header; that also keeps the free instance awake. The main
server's watchdog (app/jobs/watchdog.py) warns admins if these runs stop.

Start: uvicorn app.fetcher:app --host 0.0.0.0 --port $PORT
"""

import logging
import secrets
import threading
from datetime import datetime
from typing import Annotated

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.core.timeutil import now_ist
from app.jobs.market import ANNOUNCEMENT_HOURS, poll_announcements
from app.jobs.runner import last_success
from app.jobs.scrips import sync_due, sync_scrips

log = logging.getLogger(__name__)

setup_logging()
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
_running = threading.Lock()


def run_jobs(now: datetime) -> None:
    if not _running.acquire(blocking=False):  # the previous call is still working
        log.info("Previous run still in progress; skipping")
        return
    try:
        if now.hour in ANNOUNCEMENT_HOURS:
            poll_announcements()
        if sync_due(now, last_success("sync_scrips")):
            sync_scrips()
    finally:
        _running.release()


def _authorised(key: str | None) -> bool:
    secret = get_settings().fetcher_secret.get_secret_value()
    return bool(secret and key) and secrets.compare_digest(key, secret)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/run", status_code=202)
def run(background: BackgroundTasks, x_fetcher_key: Annotated[str | None, Header()] = None) -> dict:
    """Start a run and answer at once, so the scheduler's short timeout never cuts it off."""
    if not _authorised(x_fetcher_key):
        raise HTTPException(status_code=403)
    background.add_task(run_jobs, now_ist())
    return {"accepted": True}
