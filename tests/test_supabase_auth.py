import json
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.core.supabase_auth import AuthError, SupabaseAuth, code_challenge


def make_auth(handler):
    return SupabaseAuth(
        "https://proj.supabase.co/", "anon-key", httpx.Client(transport=httpx.MockTransport(handler))
    )


def session_response(user_id="u1"):
    user = {"id": user_id, "email": "a@example.com", "user_metadata": {"full_name": "Asha"}}
    return httpx.Response(200, json={"access_token": "t", "user": user})


def test_code_challenge_matches_rfc7636_example():
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    assert code_challenge(verifier) == "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"


def test_authorize_url_carries_pkce_challenge():
    auth = make_auth(lambda req: httpx.Response(500))
    url = urlparse(auth.authorize_url("google", "https://app.test/auth/callback", "verifier-123"))
    query = parse_qs(url.query)
    assert url.netloc == "proj.supabase.co" and url.path == "/auth/v1/authorize"
    assert query["provider"] == ["google"]
    assert query["code_challenge"] == [code_challenge("verifier-123")]
    assert query["code_challenge_method"] == ["s256"]


def test_exchange_code_posts_verifier_and_returns_user():
    seen = {}

    def handler(req):
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        seen["apikey"] = req.headers["apikey"]
        return session_response()

    user = make_auth(handler).exchange_code("the-code", "the-verifier")
    assert seen["url"] == "https://proj.supabase.co/auth/v1/token?grant_type=pkce"
    assert seen["body"] == {"auth_code": "the-code", "code_verifier": "the-verifier"}
    assert seen["apikey"] == "anon-key"
    assert (user.id, user.email, user.name) == ("u1", "a@example.com", "Asha")


def test_verify_email_code_maps_invalid_token_message():
    auth = make_auth(lambda req: httpx.Response(403, json={"msg": "Token has expired or is invalid"}))
    with pytest.raises(AuthError, match="wrong or has expired"):
        auth.verify_email_code("a@example.com", "000000")


def test_rate_limit_message():
    auth = make_auth(lambda req: httpx.Response(429, json={"msg": "rate limited"}))
    with pytest.raises(AuthError, match="Too many attempts"):
        auth.send_email_code("a@example.com")


def test_network_error_is_friendly():
    def handler(req):
        raise httpx.ConnectError("down")

    with pytest.raises(AuthError, match="Could not reach"):
        make_auth(handler).send_email_code("a@example.com")
