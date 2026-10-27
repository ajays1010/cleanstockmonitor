def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_home_renders_for_anonymous_user(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Sign in" in resp.text


def test_login_page_offers_google_and_email(client):
    resp = client.get("/login")
    assert resp.status_code == 200
    assert "/auth/google" in resp.text
    assert 'action="/auth/email"' in resp.text
