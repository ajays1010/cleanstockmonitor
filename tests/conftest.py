import pytest
from fastapi.testclient import TestClient

from app.core.profiles import Profile
from app.core.supabase_auth import AuthError, AuthUser
from app.web.deps import get_auth, get_profiles
from app.web.main import create_app


class FakeProfiles:
    def __init__(self):
        self.rows: dict[str, Profile] = {}

    def get(self, user_id):
        return self.rows.get(user_id)

    def ensure(self, user: AuthUser):
        self.rows.setdefault(user.id, Profile(user.id, user.email, user.name, is_admin=False))
        return self.rows[user.id]


class FakeAuth:
    GOOD_CODE = "123456"

    def __init__(self):
        self.sent_to: list[str] = []
        self.last_verifier: str | None = None

    def authorize_url(self, provider, redirect_to, verifier):
        self.last_verifier = verifier
        return f"https://supabase.test/auth/v1/authorize?provider={provider}"

    def exchange_code(self, auth_code, verifier):
        if auth_code != "good-code" or verifier != self.last_verifier:
            raise AuthError("Google sign-in failed.")
        return AuthUser(id="user-google", email="g@example.com", name="Gita")

    def send_email_code(self, email):
        self.sent_to.append(email)

    def verify_email_code(self, email, code):
        if code != self.GOOD_CODE:
            raise AuthError("That code is wrong or has expired. Request a new one.")
        return AuthUser(id="user-email", email=email, name=None)


@pytest.fixture
def fakes():
    return FakeAuth(), FakeProfiles()


@pytest.fixture
def client(fakes):
    auth, profiles = fakes
    app = create_app()
    app.dependency_overrides[get_auth] = lambda: auth
    app.dependency_overrides[get_profiles] = lambda: profiles
    return TestClient(app, follow_redirects=False)
