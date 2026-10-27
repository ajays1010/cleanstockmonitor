"""Wraps each scheduled job so every run is recorded in job_runs."""

import functools
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from app.core.db import get_db

log = logging.getLogger(__name__)

JobFn = Callable[[], dict[str, Any] | None]


def tracked(name: str) -> Callable[[JobFn], Callable[[], None]]:
    """Record start, finish, stats and any error of a job in job_runs.

    The job returns a dict of counts (stored as stats) or None. Exceptions are logged
    and recorded, never raised, so one failing run does not stop the scheduler.
    """

    def decorator(fn: JobFn) -> Callable[[], None]:
        @functools.wraps(fn)
        def wrapper() -> None:
            db = get_db()
            run_id = None
            try:
                run = db.table("job_runs").insert({"job": name}).execute()
                run_id = run.data[0]["id"]
            except Exception:
                log.exception("Could not record start of job %s", name)

            ok, stats, error = True, {}, None
            try:
                stats = fn() or {}
            except Exception as exc:
                ok, error = False, f"{type(exc).__name__}: {exc}"
                log.exception("Job %s failed", name)

            if run_id is not None:
                try:
                    db.table("job_runs").update(
                        {
                            "finished_at": datetime.now(UTC).isoformat(),
                            "ok": ok,
                            "stats": stats,
                            "error": error,
                        }
                    ).eq("id", run_id).execute()
                except Exception:
                    log.exception("Could not record end of job %s", name)

        return wrapper

    return decorator
