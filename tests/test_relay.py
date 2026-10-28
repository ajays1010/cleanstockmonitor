from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core.timeutil import IST
from app.web.main import create_app
from app.web.routes import relay

KEY = {"X-Fetcher-Key": "s3cret"}


def ist(hour, minute=0):
    return datetime(2026, 10, 28, hour, minute, tzinfo=IST)


@pytest.fixture
def client(monkeypatch):
    calls = {"announcements": [], "scrips": []}
    settings = SimpleNamespace(fetcher_secret=SecretStr("s3cret"))
    monkeypatch.setattr(relay, "get_settings", lambda: settings)
    monkeypatch.setattr(relay, "watched_bse_codes", lambda: ["533096", "543349"])
    monkeypatch.setattr(relay, "last_success", lambda job: ist(7))
    monkeypatch.setattr(relay, "now_ist", lambda: ist(12))
    monkeypatch.setattr(
        relay,
        "store_relayed_announcements",
        lambda rows, baseline: calls["announcements"].append((rows, baseline)),
    )
    monkeypatch.setattr(relay, "sync_scrips", lambda rows: calls["scrips"].append(len(rows)))
    return TestClient(create_app()), calls


def test_needs_the_secret(client):
    c, _ = client
    assert c.get("/relay/bse/tasks").status_code == 403
    assert c.get("/relay/bse/tasks", headers={"X-Fetcher-Key": "nope"}).status_code == 403
    assert c.post("/relay/bse/results", json={"results": {}}).status_code == 403


def test_tasks_lists_bse_requests(client):
    c, _ = client
    body = c.get("/relay/bse/tasks", headers=KEY).json()
    assert [t["id"] for t in body["tasks"]] == ["ann:533096", "ann:543349"]
    assert body["tasks"][0]["url"].endswith("/AnnSubCategoryGetData/w")
    assert body["tasks"][0]["params"]["strScrip"] == "533096"
    assert body["headers"]["Referer"] == "https://www.bseindia.com/"


def test_plan_hours_and_scrip_sync():
    assert relay.plan(ist(3), ["1"], ist(2)) == []  # night, synced since the 20:30 slot
    tasks = relay.plan(ist(21), ["1"], ist(7))  # 20:30 slot passed since the 07:00 sync
    assert [t["id"] for t in tasks] == ["ann:1", "scrips"]


def test_results_are_stored(client):
    c, calls = client
    payload = {
        "results": {
            "ann:533096": {"status": 200, "body": {"Table": [{"NEWSID": "n1"}]}},
            "ann:543349": {"status": 200, "body": "No Record Found!"},
            "ann:500325": {"status": 403, "body": "<HTML>Access Denied</HTML>"},
            "ann:500209": {"error": "ConnectTimeout"},
            "scrips": {"status": 200, "body": [{"SCRIP_CD": "1"}] * 3},
        },
        "baseline": True,
    }
    assert c.post("/relay/bse/results", json=payload, headers=KEY).status_code == 202
    [(rows, baseline)] = calls["announcements"]
    assert rows == {"533096": [{"NEWSID": "n1"}], "543349": [], "500325": None, "500209": None}
    assert baseline is True and calls["scrips"] == [3]


def test_failed_scrip_list_is_not_synced(client):
    c, calls = client
    payload = {"results": {"scrips": {"status": 403, "body": "denied"}}}
    assert c.post("/relay/bse/results", json=payload, headers=KEY).status_code == 202
    assert calls == {"announcements": [], "scrips": []}
