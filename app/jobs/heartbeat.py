"""Proves the worker and database are alive; the admin page shows the last run."""

from app.jobs.runner import tracked


@tracked("heartbeat")
def heartbeat() -> dict:
    return {"alive": True}
