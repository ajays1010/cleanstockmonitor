import pytest

from app.bot.handlers import BotHandlers

BOT = "StockBuzzBot"
ME = {"id": 111, "first_name": "Ajay", "last_name": "Singhal"}
PRIVATE = {"id": 111, "type": "private"}
GROUP = {"id": -2001, "type": "group", "title": "Family Investors"}


def msg(text, chat=PRIVATE, sender=ME):
    return {"update_id": 1, "message": {"text": text, "chat": chat, "from": sender}}


@pytest.fixture
def bot(links, watchlist):
    return BotHandlers(links, watchlist, BOT)


def text_of(replies):
    assert len(replies) == 1
    return replies[0].text


def test_start_with_valid_code_links_private_chat(bot, links):
    code = links.create_code("user-1")
    reply = text_of(bot.handle(msg(f"/start {code}")))
    assert "Connected" in reply
    chat = links.chats_for_user("user-1")[0]
    assert (chat.chat_id, chat.label) == (111, "Ajay Singhal")


def test_code_works_only_once(bot, links):
    code = links.create_code("user-1")
    bot.handle(msg(f"/start {code}"))
    assert "expired or was already used" in text_of(bot.handle(msg(f"/start {code}", chat=GROUP)))


def test_expired_code_is_refused(bot, links):
    links.codes["old"] = ("user-1", True)
    assert "expired" in text_of(bot.handle(msg("/start old")))
    assert links.chats == {}


def test_group_link_uses_group_title(bot, links):
    code = links.create_code("user-1")
    reply = text_of(bot.handle(msg(f"/start@{BOT} {code}", chat=GROUP)))
    assert "to this group" in reply
    assert links.chats_for_user("user-1")[0].label == "Family Investors"


def test_commands_for_another_bot_are_ignored(bot):
    assert bot.handle(msg("/list@SomeOtherBot", chat=GROUP)) == []


def test_plain_text_gets_help_in_private_but_silence_in_groups(bot):
    assert "Connect Telegram" in text_of(bot.handle(msg("hello")))
    assert bot.handle(msg("hello", chat=GROUP)) == []


def test_unlinked_chat_cannot_list(bot):
    assert "not connected" in text_of(bot.handle(msg("/list")))


def test_list_add_remove_in_private_chat(bot, links, watchlist):
    links.link("user-1", 111, "Ajay")
    assert "empty" in text_of(bot.handle(msg("/list")))

    assert "Added Tata Power" in text_of(bot.handle(msg("/add TATAPOWER")))
    assert "Added Reliance" in text_of(bot.handle(msg("/add 500325")))
    listing = text_of(bot.handle(msg("/list")))
    assert "Reliance Industries Ltd (RELIANCE)" in listing and "Tata Power" in listing

    assert "Removed Tata Power" in text_of(bot.handle(msg("/remove tatapower")))
    assert [i.scrip.nse_symbol for i in watchlist.list("user-1")] == ["RELIANCE"]


def test_ambiguous_add_asks_which_one(bot, links, watchlist):
    links.link("user-1", 111, "Ajay")
    reply = text_of(bot.handle(msg("/add tata")))
    assert "Which one?" in reply and "/add TCS" in reply and "/add TATAPOWER" in reply
    assert watchlist.list("user-1") == []


def test_add_unknown_scrip(bot, links):
    links.link("user-1", 111, "Ajay")
    assert "No scrip matches" in text_of(bot.handle(msg("/add zzzz")))


def test_watchlist_changes_refused_in_groups(bot, links, watchlist):
    links.link("user-1", GROUP["id"], "Family Investors")
    assert "private chat" in text_of(bot.handle(msg("/add TCS", chat=GROUP)))
    assert watchlist.list("user-1") == []


def test_names_are_html_escaped(bot, links):
    code = links.create_code("user-1")
    bot.handle(msg(f"/start {code}"))
    assert "<script>" not in text_of(bot.handle(msg("/add <script>")))


def test_stop_disconnects(bot, links):
    links.link("user-1", 111, "Ajay")
    assert "Disconnected" in text_of(bot.handle(msg("/stop")))
    assert links.chats == {}


def test_bot_removed_from_group_unlinks_it(bot, links):
    links.link("user-1", GROUP["id"], "Family Investors")
    update = {"update_id": 2, "my_chat_member": {"chat": GROUP, "new_chat_member": {"status": "kicked"}}}
    assert bot.handle(update) == []
    assert links.chats == {}
