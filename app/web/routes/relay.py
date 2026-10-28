"""Home relay for BSE. BSE refuses this server (its bot protection blocks cloud hosts), so
a small device on a home connection (deploy/pi/bse_relay.py on a Raspberry Pi) fetches
BSE for it.

The server decides what is fetched: GET /relay/bse/tasks lists the BSE requests to make
now (announcements of watched scrips 07:00-23:59 IST, and the scrip list once a sync slot
has passed). The device makes them as given and POSTs the raw answers to
/relay/bse/results. Both need the X-Fetcher-Key header (FETCHER_SECRET); the device holds
no database keys, and the relay script never needs changing when the app does.
"""

import logging
import secrets
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel

from app.core.config import get_settings
from app.core.timeutil import now_ist
from app.jobs.market import ANNOUNCEMENT_HOURS, store_relayed_announcements, watched_bse_codes
from app.jobs.runner import last_success
from app.jobs.scrips import sync_due, sync_scrips
from app.sources import bse
from app.sources.scrip_master import BSE_LIST, BSE_LIST_PARAMS

log = logging.getLogger(__name__)

router = APIRouter(prefix="/relay/bse")

DELAY = 0.3  # seconds between BSE requests, to be gentle with BSE


def require_relay(x_fetcher_key: Annotated[str | None, Header()] = None) -> None:
    secret = get_settings().fetcher_secret.get_secret_value()
    if not (secret and x_fetcher_key and secrets.compare_digest(x_fetcher_key, secret)):
        raise HTTPException(status_code=403)


RelayDep = Annotated[None, Depends(require_relay)]


def plan(now: datetime, bse_codes: list[str], last_sync: datetime | None) -> list[dict]:
    tasks = []
    if now.hour in ANNOUNCEMENT_HOURS:
        tasks += [
            {"id": f"ann:{code}", "url": bse.API_URL, "params": bse.announcement_params(code)}
            for code in bse_codes
        ]
    if sync_due(now, last_sync):
        tasks.append({"id": "scrips", "url": BSE_LIST, "params": BSE_LIST_PARAMS})
    return tasks


class Result(BaseModel):
    status: int | None = None
    body: Any = None
    error: str | None = None


class Results(BaseModel):
    results: dict[str, Result]
    baseline: bool = False


def announcement_rows(result: Result) -> list[dict] | None:
    """BSE's rows from one relayed answer, [] when it had none, None when BSE failed."""
    if result.error or result.status != 200:
        return None
    if isinstance(result.body, dict):
        return result.body.get("Table") or []
    if isinstance(result.body, str):  # BSE answers a bare "No Record Found!" when there is nothing
        return []
    return None


@router.get("/tasks")
def tasks(_: RelayDep) -> dict:
    return {
        "tasks": plan(now_ist(), watched_bse_codes(), last_success("sync_scrips")),
        "headers": bse.HEADERS,
        "delay": DELAY,
    }


@router.post("/results", status_code=202)
def results(_: RelayDep, payload: Results, background: BackgroundTasks) -> dict:
    """Store what the relay fetched, after answering, so the device is never kept waiting."""
    announcements = {
        key.removeprefix("ann:"): announcement_rows(result)
        for key, result in payload.results.items()
        if key.startswith("ann:")
    }
    if announcements:
        background.add_task(store_relayed_announcements, announcements, payload.baseline)
    scrip_list = payload.results.get("scrips")
    if scrip_list is not None:
        if scrip_list.status == 200 and isinstance(scrip_list.body, list):
            background.add_task(sync_scrips, scrip_list.body)
        else:
            log.warning("Relay could not fetch the BSE scrip list: %s", scrip_list.error or scrip_list.status)
    return {"accepted": True}
