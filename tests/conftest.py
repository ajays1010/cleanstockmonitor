from dataclasses import replace
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from app.core.profiles import Profile
from app.core.supabase_auth import AuthError, AuthUser
from app.core.watchlist import Scrip, WatchItem, WatchlistError, rank_matches
from app.web.deps import get_auth, get_profiles, get_watchlist
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


SCRIPS = [
    Scrip(1, "500325", "Reliance Industries Ltd", "RELIANCE"),
    Scrip(2, "500400", "Tata Power Company Ltd", "TATAPOWER"),
    Scrip(3, "532540", "Tata Consultancy Services Ltd", "TCS"),
]


class FakeWatchlist:
    def __init__(self):
        self.rows: dict[int, tuple[str, WatchItem]] = {}
        self.next_id = 1

    def search(self, query, limit=10):
        q = query.lower()
        hits = [
            s for s in SCRIPS if q in s.name.lower() or q in (s.nse_symbol or "").lower() or q in s.bse_code
        ]
        return rank_matches(q, hits, limit)

    def list(self, user_id):
        return sorted((i for u, i in self.rows.values() if u == user_id), key=lambda i: i.scrip.name)

    def add(self, user_id, scrip_id):
        scrip = next((s for s in SCRIPS if s.id == scrip_id), None)
        if scrip is None:
            raise WatchlistError("That scrip was not found.")
        if any(i.scrip.id == scrip_id for i in self.list(user_id)):
            return
        self.rows[self.next_id] = (user_id, WatchItem(self.next_id, scrip, Decimal("5"), None, None, []))
        self.next_id += 1

    def remove(self, user_id, item_id):
        if self.rows.get(item_id, (None,))[0] == user_id:
            del self.rows[item_id]

    def update(self, user_id, item_id, settings):
        owner, item = self.rows.get(item_id, (None, None))
        if owner != user_id:
            raise WatchlistError("That watchlist entry was not found.")
        self.rows[item_id] = (
            user_id,
            replace(
                item,
                spike_pct=settings.spike_pct,
                price_above=settings.price_above,
                price_below=settings.price_below,
                categories=settings.categories,
            ),
        )


@pytest.fixture
def fakes():
    return FakeAuth(), FakeProfiles()


@pytest.fixture
def watchlist():
    return FakeWatchlist()


@pytest.fixture
def client(fakes, watchlist):
    auth, profiles = fakes
    app = create_app()
    app.dependency_overrides[get_auth] = lambda: auth
    app.dependency_overrides[get_profiles] = lambda: profiles
    app.dependency_overrides[get_watchlist] = lambda: watchlist
    return TestClient(app, follow_redirects=False)


@pytest.fixture
def signed_in(client):
    client.post("/auth/email", data={"email": "a@example.com"})
    client.post("/auth/email/verify", data={"code": FakeAuth.GOOD_CODE})
    client.headers["HX-Request"] = "true"
    return client
