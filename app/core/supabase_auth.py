"""Small client for the Supabase Auth (GoTrue) REST API.

Supabase is used only to prove who the user is: Google sign-in with PKCE, or a
6-digit code sent by email. The web app then keeps its own signed session, so
Supabase access and refresh tokens are never stored.
"""

import base64
import hashlib
import secrets
from dataclasses import dataclass
from urllib.parse import urlencode

import httpx


class AuthError(Exception):
    """A sign-in step failed; the message is safe to show to the user."""


@dataclass(frozen=True)
class AuthUser:
    id: str
    email: str | None
    name: str | None


def new_code_verifier() -> str:
    return secrets.token_urlsafe(48)  # 64 characters, within PKCE's 43 to 128


def code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


class SupabaseAuth:
    def __init__(self, supabase_url: str, anon_key: str, http: httpx.Client | None = None):
        self.base = supabase_url.rstrip("/") + "/auth/v1"
        self.anon_key = anon_key
        self.http = http or httpx.Client(timeout=15)

    def authorize_url(self, provider: str, redirect_to: str, verifier: str) -> str:
        query = urlencode(
            {
                "provider": provider,
                "redirect_to": redirect_to,
                "code_challenge": code_challenge(verifier),
                "code_challenge_method": "s256",
            }
        )
        return f"{self.base}/authorize?{query}"

    def exchange_code(self, auth_code: str, verifier: str) -> AuthUser:
        data = self._post("/token?grant_type=pkce", {"auth_code": auth_code, "code_verifier": verifier})
        return _user_from_session(data)

    def send_email_code(self, email: str) -> None:
        self._post("/otp", {"email": email, "create_user": True})

    def verify_email_code(self, email: str, code: str) -> AuthUser:
        data = self._post("/verify", {"type": "email", "email": email, "token": code})
        return _user_from_session(data)

    def _post(self, path: str, body: dict) -> dict:
        try:
            resp = self.http.post(
                self.base + path,
                json=body,
                headers={"apikey": self.anon_key, "Authorization": f"Bearer {self.anon_key}"},
            )
        except httpx.HTTPError as exc:
            raise AuthError("Could not reach the sign-in service. Please try again.") from exc
        try:
            data = resp.json() if resp.content else {}
        except ValueError:
            data = {}
        if resp.status_code >= 400:
            raise AuthError(_error_message(resp.status_code, data))
        return data


def _user_from_session(data: dict) -> AuthUser:
    user = data.get("user") or {}
    if not user.get("id"):
        raise AuthError("Sign-in did not return a user. Please try again.")
    meta = user.get("user_metadata") or {}
    return AuthUser(id=user["id"], email=user.get("email"), name=meta.get("full_name") or meta.get("name"))


def _error_message(status: int, data: dict) -> str:
    if status == 429:
        return "Too many attempts. Please wait a minute and try again."
    message = data.get("msg") or data.get("error_description") or data.get("message")
    if message and "expired or is invalid" in message:
        return "That code is wrong or has expired. Request a new one."
    return message or "Sign-in failed. Please try again."
