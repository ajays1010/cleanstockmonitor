"""Telegram bot commands. Pure functions: take an update, return the replies to send.

/start <code>  link this chat (private, or a group via the startgroup link)
/list          the watchlist of the account linked here
/add <scrip>   add by NSE symbol, BSE code or name (private chats with one account)
/remove <scrip>
/stop          disconnect this chat
/help
"""

from dataclasses import dataclass

from app.core.config import get_settings
from app.core.telegram import esc
from app.core.telegram_links import TelegramLinkStore
from app.core.watchlist import WatchItem, WatchlistError, WatchlistStore


@dataclass(frozen=True)
class Reply:
    chat_id: int
    text: str


def help_text() -> str:
    site = get_settings().app_base_url
    return (
        "<b>StockMonitor</b> sends BSE announcements and price alerts for your watchlist.\n\n"
        f"To connect this chat, sign in at {esc(site)} and click <b>Connect Telegram</b>.\n\n"
        "/list – your watchlist\n"
        "/add &lt;symbol or BSE code&gt; – watch a scrip\n"
        "/remove &lt;symbol or BSE code&gt; – stop watching\n"
        "/stop – disconnect this chat"
    )


def _label(chat: dict, sender: dict) -> str:
    if chat.get("type") == "private":
        name = " ".join(p for p in (sender.get("first_name"), sender.get("last_name")) if p)
        return name or sender.get("username") or "Private chat"
    return chat.get("title") or "Group"


def _parse_command(text: str, bot_name: str) -> tuple[str, str] | None:
    """'/add@MyBot tcs' -> ('add', 'tcs'). None if not a command or meant for another bot."""
    if not text.startswith("/"):
        return None
    head, _, arg = text.partition(" ")
    command, _, target = head[1:].partition("@")
    if target and target.lower() != bot_name.lower():
        return None
    return command.lower(), arg.strip()


def _scrip_label(item: WatchItem) -> str:
    s = item.scrip
    return f"{esc(s.name)} ({esc(s.nse_symbol or 'BSE ' + s.bse_code)})"


class BotHandlers:
    def __init__(self, links: TelegramLinkStore, watchlist: WatchlistStore, bot_name: str):
        self.links = links
        self.watchlist = watchlist
        self.bot_name = bot_name

    def handle(self, update: dict) -> list[Reply]:
        if member := update.get("my_chat_member"):
            status = member.get("new_chat_member", {}).get("status")
            if status in ("left", "kicked"):
                self.links.unlink_chat(member["chat"]["id"])
            return []

        message = update.get("message") or {}
        text = (message.get("text") or "").strip()
        chat = message.get("chat") or {}
        if not chat or not text:
            return []
        chat_id = chat["id"]
        private = chat.get("type") == "private"

        parsed = _parse_command(text, self.bot_name)
        if parsed is None:
            return [Reply(chat_id, help_text())] if private else []
        command, arg = parsed

        if command == "start":
            return [Reply(chat_id, self._start(chat, message.get("from") or {}, arg))]
        if command == "stop":
            removed = self.links.unlink_chat(chat_id)
            return [
                Reply(
                    chat_id,
                    "Disconnected. No more alerts will come here."
                    if removed
                    else "This chat was not connected.",
                )
            ]
        if command == "list":
            return [Reply(chat_id, self._list(chat_id))]
        if command in ("add", "remove"):
            return [Reply(chat_id, self._change(chat_id, private, command, arg))]
        if command == "help" or private:
            return [Reply(chat_id, help_text())]
        return []

    def _start(self, chat: dict, sender: dict, code: str) -> str:
        if not code:
            if self.links.users_for_chat(chat["id"]):
                return "This chat is connected. Send /list to see your watchlist."
            return help_text()
        user_id = self.links.consume_code(code)
        if user_id is None:
            return (
                "This link has expired or was already used. "
                "Click <b>Connect Telegram</b> on the website again."
            )
        self.links.link(user_id, chat["id"], _label(chat, sender))
        where = "here" if chat.get("type") == "private" else "to this group"
        return f"✅ Connected. Alerts for your watchlist will now come {where}.\n\nSend /list to see it."

    def _list(self, chat_id: int) -> str:
        users = self.links.users_for_chat(chat_id)
        if not users:
            return "This chat is not connected yet.\n\n" + help_text()
        seen, lines = set(), []
        for user_id in users:
            for item in self.watchlist.list(user_id):
                if item.scrip.id not in seen:
                    seen.add(item.scrip.id)
                    lines.append(f"• {_scrip_label(item)}")
        if not lines:
            return "The watchlist is empty. Add a scrip with /add &lt;symbol&gt;."
        return "<b>Watchlist</b>\n" + "\n".join(lines)

    def _change(self, chat_id: int, private: bool, command: str, arg: str) -> str:
        users = self.links.users_for_chat(chat_id)
        if not users:
            return "This chat is not connected yet.\n\n" + help_text()
        if not private or len(users) != 1:
            return "Change the watchlist from your private chat with me, or on the website."
        if not arg:
            return f"Send /{command} followed by an NSE symbol or BSE code, for example /{command} TATAPOWER."
        user_id = users[0]
        return self._add(user_id, arg) if command == "add" else self._remove(user_id, arg)

    def _add(self, user_id: str, query: str) -> str:
        matches = self.watchlist.search(query, limit=5)
        if not matches:
            return f"No scrip matches “{esc(query)}”."
        q = query.strip().lower()
        exact = [s for s in matches if q in (s.bse_code, (s.nse_symbol or "").lower())]
        if len(exact) != 1 and len(matches) != 1:
            options = "\n".join(f"• {esc(s.name)} – /add {esc(s.nse_symbol or s.bse_code)}" for s in matches)
            return f"Which one?\n{options}"
        scrip = exact[0] if exact else matches[0]
        try:
            self.watchlist.add(user_id, scrip.id)
        except WatchlistError as exc:
            return esc(str(exc))
        return (
            f"Added {esc(scrip.name)}. It starts with a 5% move alert and all announcements; "
            "change that on the website."
        )

    def _remove(self, user_id: str, query: str) -> str:
        q = query.strip().lower()
        items = self.watchlist.list(user_id)
        hits = [i for i in items if q in (i.scrip.bse_code, (i.scrip.nse_symbol or "").lower())]
        if not hits:
            hits = [i for i in items if q in i.scrip.name.lower()]
        if not hits:
            return f"“{esc(query)}” is not on your watchlist. Send /list to see it."
        if len(hits) > 1:
            options = "\n".join(
                f"• {_scrip_label(i)} – /remove {esc(i.scrip.nse_symbol or i.scrip.bse_code)}" for i in hits
            )
            return f"Which one?\n{options}"
        self.watchlist.remove(user_id, hits[0].id)
        return f"Removed {esc(hits[0].scrip.name)}."
