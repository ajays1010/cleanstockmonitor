def sign_in_by_email(client, email="a@example.com", code="123456"):
    client.post("/auth/email", data={"email": email})
    return client.post("/auth/email/verify", data={"code": code})


def test_dashboard_redirects_anonymous_user_to_login(client):
    resp = client.get("/dashboard")
    assert resp.status_code == 303
    assert resp.headers["location"] == "/login?next=/dashboard"


def test_email_code_sign_in(client, fakes):
    auth, profiles = fakes
    resp = client.post("/auth/email", data={"email": " A@Example.com "})
    assert resp.status_code == 200
    assert "6-digit code" in resp.text
    assert auth.sent_to == ["a@example.com"]

    resp = client.post("/auth/email/verify", data={"code": "123 456"})
    assert resp.status_code == 303
    assert resp.headers["location"] == "/dashboard"
    assert "user-email" in profiles.rows
    assert client.get("/dashboard").status_code == 200


def test_wrong_email_code_stays_on_code_page(client):
    resp = sign_in_by_email(client, code="000000")
    assert resp.status_code == 400
    assert "wrong or has expired" in resp.text
    assert client.get("/dashboard").status_code == 303


def test_invalid_email_is_rejected_before_calling_supabase(client, fakes):
    auth, _ = fakes
    resp = client.post("/auth/email", data={"email": "not-an-email"})
    assert resp.status_code == 400
    assert auth.sent_to == []


def test_verify_without_email_step_is_rejected(client):
    resp = client.post("/auth/email/verify", data={"code": "123456"})
    assert resp.status_code == 400
    assert "expired" in resp.text


def test_google_sign_in_with_pkce(client):
    resp = client.get("/auth/google")
    assert resp.status_code == 303
    assert resp.headers["location"].startswith("https://supabase.test/auth/v1/authorize")

    resp = client.get("/auth/callback", params={"code": "good-code"})
    assert resp.status_code == 303
    assert client.get("/dashboard").status_code == 200


def test_google_callback_without_started_flow_fails(client):
    resp = client.get("/auth/callback", params={"code": "good-code"})
    assert resp.status_code == 400
    assert client.get("/dashboard").status_code == 303


def test_google_callback_error_is_shown(client):
    client.get("/auth/google")
    resp = client.get("/auth/callback", params={"error_description": "access_denied"})
    assert resp.status_code == 400
    assert "access_denied" in resp.text


def test_login_returns_to_requested_page(client):
    client.get("/login", params={"next": "/dashboard?tab=telegram"})
    resp = sign_in_by_email(client)
    assert resp.headers["location"] == "/dashboard?tab=telegram"


def test_login_ignores_redirect_to_other_sites(client):
    for target in ["https://evil.example", "//evil.example", "/\\evil.example"]:
        client.get("/login", params={"next": target})
        resp = sign_in_by_email(client)
        assert resp.headers["location"] == "/dashboard"
        client.post("/logout")


def test_logout_ends_session(client):
    sign_in_by_email(client)
    resp = client.post("/logout")
    assert resp.status_code == 303
    assert client.get("/dashboard").status_code == 303


def test_deleted_user_is_signed_out(client, fakes):
    _, profiles = fakes
    sign_in_by_email(client)
    profiles.rows.clear()
    assert client.get("/dashboard").status_code == 303
