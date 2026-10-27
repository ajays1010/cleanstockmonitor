from app.web.deps import get_bot_username


def user_id(fakes):
    return next(iter(fakes[1].rows))


def test_dashboard_offers_connect(signed_in):
    resp = signed_in.get("/dashboard")
    assert "Connect Telegram" in resp.text


def test_connect_shows_one_time_links_and_polls(signed_in, links, fakes):
    resp = signed_in.post("/telegram/connect")
    code = next(iter(links.codes))
    assert links.codes[code][0] == user_id(fakes)
    assert f"https://t.me/StockBuzzTestBot?start={code}" in resp.text
    assert f"https://t.me/StockBuzzTestBot?startgroup={code}" in resp.text
    assert 'hx-trigger="every 3s"' in resp.text


def test_panel_stops_polling_once_chat_is_linked(signed_in, links, fakes):
    signed_in.post("/telegram/connect")
    assert 'hx-trigger="every 3s"' in signed_in.get("/telegram/panel").text

    links.link(user_id(fakes), 111, "Ajay Singhal")  # what the bot does on /start <code>
    resp = signed_in.get("/telegram/panel")
    assert "Connected. Alerts will go to this chat." in resp.text
    assert "Ajay Singhal" in resp.text
    assert "every 3s" not in resp.text


def test_cancel_stops_waiting(signed_in):
    signed_in.post("/telegram/connect")
    resp = signed_in.post("/telegram/cancel")
    assert "every 3s" not in resp.text and "Connect Telegram" in resp.text


def test_send_test_message(signed_in, links, sender, fakes):
    links.link(user_id(fakes), 111, "Ajay Singhal")
    link_id = next(iter(links.chats))
    resp = signed_in.post(f"/telegram/{link_id}/test")
    assert "Test message sent to Ajay Singhal" in resp.text
    assert sender.sent[0][0] == 111


def test_test_message_failure_is_shown(signed_in, links, sender, fakes):
    links.link(user_id(fakes), 111, "Ajay")
    sender.fail_with = "Forbidden: bot was blocked by the user"
    resp = signed_in.post(f"/telegram/{next(iter(links.chats))}/test")
    assert resp.status_code == 400 and "blocked by the user" in resp.text


def test_cannot_test_or_remove_another_users_chat(signed_in, links, sender):
    links.link("someone-else", 999, "Not yours")
    link_id = next(iter(links.chats))
    assert signed_in.post(f"/telegram/{link_id}/test").status_code == 400
    assert sender.sent == []
    signed_in.post(f"/telegram/{link_id}/remove")
    assert link_id in links.chats


def test_disconnect(signed_in, links, fakes):
    links.link(user_id(fakes), 111, "Ajay")
    resp = signed_in.post(f"/telegram/{next(iter(links.chats))}/remove")
    assert "Disconnected." in resp.text and links.chats == {}


def test_panel_explains_missing_bot_token(signed_in):
    signed_in.app.dependency_overrides[get_bot_username] = lambda: None
    resp = signed_in.get("/dashboard")
    assert "TELEGRAM_BOT_TOKEN" in resp.text and "Connect Telegram" not in resp.text
