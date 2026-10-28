from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app import fetcher, worker
from app.core.timeutil import IST
from app.jobs import watchdog


def ist(day, hour, minute=0):
    return datetime(2026, 10, day, hour, minute, tzinfo=IST)


# ---- fetcher


@pytest.fixture
def calls(monkeypatch):
    seen = []
    monkeypatch.setattr(fetcher, "poll_announcements", lambda: seen.append("announcements"))
    monkeypatch.setattr(fetcher, "sync_scrips", lambda: seen.append("scrips"))
    monkeypatch.setattr(fetcher, "last_success", lambda job: ist(28, 7))
    settings = SimpleNamespace(fetcher_secret=SecretStr("s3cret"))
    monkeypatch.setattr(fetcher, "get_settings", lambda: settings)
    return seen


def test_run_needs_the_secret(calls):
    client = TestClient(fetcher.app)
    assert client.post("/run").status_code == 403
    assert client.post("/run", headers={"X-Fetcher-Key": "wrong"}).status_code == 403
    assert client.get("/health").json() == {"ok": True}
    assert calls == []


def test_run_polls_in_the_background(calls, monkeypatch):
    monkeypatch.setattr(fetcher, "now_ist", lambda: ist(28, 12))
    resp = TestClient(fetcher.app).post("/run", headers={"X-Fetcher-Key": "s3cret"})
    assert resp.status_code == 202 and calls == ["announcements"]


def test_no_secret_configured_refuses_everyone(calls, monkeypatch):
    monkeypatch.setattr(fetcher, "get_settings", lambda: SimpleNamespace(fetcher_secret=SecretStr("")))
    assert TestClient(fetcher.app).post("/run", headers={"X-Fetcher-Key": ""}).status_code == 403


def test_run_jobs_hours_and_sync_slots(calls, monkeypatch):
    fetcher.run_jobs(ist(28, 3))  # night: no announcements; the last sync (07:00) is newer than any slot
    assert calls == []
    fetcher.run_jobs(ist(28, 21))  # 20:30 slot passed since the 07:00 sync
    assert calls == ["announcements", "scrips"]


def test_sync_due():
    assert fetcher.sync_due(ist(28, 6, 29), ist(27, 20, 31)) is False
    assert fetcher.sync_due(ist(28, 6, 31), ist(27, 20, 31)) is True
    assert fetcher.sync_due(ist(28, 2), ist(27, 6, 40)) is True  # missed the 20:30 slot
    assert fetcher.sync_due(ist(28, 12), None) is True


def test_overlapping_runs_are_skipped(calls):
    with fetcher._running:
        fetcher.run_jobs(ist(28, 12))
    assert calls == []


# ---- watchdog


def test_checks():
    now = ist(28, 12)
    fresh, scrips_ok = now - timedelta(minutes=4), now - timedelta(hours=30)
    assert watchdog.checks(now, fresh, scrips_ok) == {"announcements": None, "scrips": None}
    found = watchdog.checks(now, now - timedelta(minutes=20), now - timedelta(days=4), source="NSE")
    assert "NSE announcement check since 28 Oct 11:40 IST" in found["announcements"]
    assert "scrip list" in found["scrips"]
    assert "yet" in watchdog.checks(now, None, scrips_ok)["announcements"]
    # Overnight, announcements are not judged at all.
    assert "announcements" not in watchdog.checks(ist(28, 3), None, scrips_ok)


@pytest.mark.parametrize(
    ("source", "job"), [("bse", "poll_announcements"), ("nse", "poll_nse_announcements")]
)
def test_watchdog_warns_once_then_reports_recovery(monkeypatch, source, job):
    sent = []
    last = {job: None, "sync_scrips": ist(28, 6, 30)}
    monkeypatch.setattr(watchdog, "notify_admins", sent.append)
    monkeypatch.setattr(watchdog, "last_success", lambda name: last.get(name))
    monkeypatch.setattr(watchdog, "now_ist", lambda: ist(28, 12))
    monkeypatch.setattr(watchdog, "get_settings", lambda: SimpleNamespace(announcement_source=source))
    monkeypatch.setattr(watchdog, "_warned", set())
    monkeypatch.setattr("app.jobs.runner.get_db", MagicMock)  # job_runs bookkeeping

    watchdog.bse_watchdog()
    watchdog.bse_watchdog()
    assert len(sent) == 1 and sent[0].startswith(f"⚠️ No successful {source.upper()} announcement check")
    last[job] = ist(28, 11, 58)
    watchdog.bse_watchdog()
    assert sent[-1] == f"✅ {source.upper()} announcements working again."


# ---- worker modes


@pytest.mark.parametrize(
    ("source", "fetch", "present", "absent"),
    [
        ("bse", "here", {"poll_announcements", "sync_scrips"}, {"bse_watchdog", "poll_nse_announcements"}),
        ("bse", "remote", {"bse_watchdog"}, {"poll_announcements", "sync_scrips", "poll_nse_announcements"}),
        ("nse", "remote", {"bse_watchdog", "poll_nse_announcements"}, {"poll_announcements", "sync_scrips"}),
        ("nse", "here", {"poll_nse_announcements", "sync_scrips"}, {"poll_announcements", "bse_watchdog"}),
    ],
)
def test_worker_jobs_follow_settings(monkeypatch, source, fetch, present, absent):
    settings = SimpleNamespace(announcement_source=source, bse_fetch=fetch)
    monkeypatch.setattr(worker, "get_settings", lambda: settings)
    ids = {job.id for job in worker.build_scheduler().get_jobs()}
    assert present <= ids and not (absent & ids) and "fan_out" in ids
