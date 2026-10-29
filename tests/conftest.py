from dataclasses import replace
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from app.core.profiles import Profile
from app.core.supabase_auth import AuthError, AuthUser
from app.core.telegram import SendResult
from app.core.telegram_links import LinkedChat
from app.core.watchlist import Scrip, WatchItem, WatchlistError, rank_matches
from app.web.deps import (
    get_auth,
    get_bot_username,
    get_prices,
    get_profiles,
    get_sender,
    get_telegram_links,
    get_watchlist,
)
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
                volume_multiple=settings.volume_multiple,
                volume_above=settings.volume_above,
                dma_alerts=settings.dma_alerts,
            ),
        )


class FakeLinks:
    def __init__(self):
        self.codes: dict[str, tuple[str, bool]] = {}  # code -> (user_id, expired)
        self.chats: dict[int, LinkedChat] = {}
        self.next_id = 1

    def create_code(self, user_id):
        self.codes = {c: v for c, v in self.codes.items() if v[0] != user_id}
        code = f"code-{len(self.codes) + 1}-{user_id}"
        self.codes[code] = (user_id, False)
        return code

    def consume_code(self, code):
        user_id, expired = self.codes.pop(code, (None, True))
        return None if expired else user_id

    def link(self, user_id, chat_id, label):
        for row in self.chats.values():
            if (row.user_id, row.chat_id) == (user_id, chat_id):
                return
        self.chats[self.next_id] = LinkedChat(self.next_id, user_id, chat_id, label)
        self.next_id += 1

    def chats_for_user(self, user_id):
        return [c for c in self.chats.values() if c.user_id == user_id]

    def users_for_chat(self, chat_id):
        return [c.user_id for c in self.chats.values() if c.chat_id == chat_id]

    def get_for_user(self, user_id, link_id):
        chat = self.chats.get(link_id)
        return chat if chat and chat.user_id == user_id else None

    def remove_for_user(self, user_id, link_id):
        if self.get_for_user(user_id, link_id):
            del self.chats[link_id]

    def unlink_chat(self, chat_id):
        gone = [i for i, c in self.chats.items() if c.chat_id == chat_id]
        for i in gone:
            del self.chats[i]
        return len(gone)


class FakeSender:
    def __init__(self):
        self.sent: list[tuple[int, str]] = []
        self.fail_with: str | None = None

    def __call__(self, chat_id, text):
        if self.fail_with:
            return SendResult(ok=False, error=self.fail_with)
        self.sent.append((chat_id, text))
        return SendResult(ok=True)


class FakePrices:
    def __init__(self):
        self.views_by_scrip: dict = {}
        self.refreshed: list[int] = []

    def views(self, scrip_ids):
        return {i: v for i, v in self.views_by_scrip.items() if i in scrip_ids}

    def refresh(self, scrip_id):
        self.refreshed.append(scrip_id)


@pytest.fixture
def fakes():
    return FakeAuth(), FakeProfiles()


@pytest.fixture
def prices():
    return FakePrices()


@pytest.fixture
def watchlist():
    return FakeWatchlist()


@pytest.fixture
def links():
    return FakeLinks()


@pytest.fixture
def sender():
    return FakeSender()


@pytest.fixture
def client(fakes, watchlist, links, sender, prices):
    auth, profiles = fakes
    app = create_app()
    app.dependency_overrides[get_auth] = lambda: auth
    app.dependency_overrides[get_profiles] = lambda: profiles
    app.dependency_overrides[get_watchlist] = lambda: watchlist
    app.dependency_overrides[get_prices] = lambda: prices
    app.dependency_overrides[get_telegram_links] = lambda: links
    app.dependency_overrides[get_bot_username] = lambda: "StockBuzzTestBot"
    app.dependency_overrides[get_sender] = lambda: sender
    return TestClient(app, follow_redirects=False)


@pytest.fixture
def signed_in(client):
    client.post("/auth/email", data={"email": "a@example.com"})
    client.post("/auth/email/verify", data={"code": FakeAuth.GOOD_CODE})
    client.headers["HX-Request"] = "true"
    return client
