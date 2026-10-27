from fastapi.testclient import TestClient

from app.web.main import create_app


def test_health():
    client = TestClient(create_app())
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_home_renders_for_anonymous_user():
    client = TestClient(create_app())
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Sign in" in resp.text
