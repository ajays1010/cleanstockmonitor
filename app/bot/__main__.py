"""Bot process: long-polls Telegram for commands. Start with `python -m app.bot`.

Long polling needs no public URL or webhook, so the same process works on a laptop
and on the VPS. Exactly one copy may run per bot token.
"""

import logging
import time

import httpx

from app.bot.handlers import BotHandlers
from app.core.config import get_settings
from app.core.db import get_db
from app.core.logging import setup_logging
from app.core.telegram import TelegramError, bot_username, call, send_message
from app.core.telegram_links import TelegramLinkStore
from app.core.watchlist import WatchlistStore

log = logging.getLogger("app.bot")

POLL_TIMEOUT = 50  # seconds Telegram holds the request open waiting for updates


def run() -> None:
    setup_logging()
    get_settings().check_production()
    name = bot_username()
    call("deleteWebhook", {"drop_pending_updates": False})  # polling and a webhook cannot coexist
    handlers = BotHandlers(TelegramLinkStore(get_db), WatchlistStore(get_db), name)
    log.info("Bot @%s polling for updates", name)

    offset = None
    backoff = 1
    with httpx.Client(timeout=POLL_TIMEOUT + 10) as http:
        while True:
            try:
                params = {"timeout": POLL_TIMEOUT, "allowed_updates": ["message", "my_chat_member"]}
                if offset is not None:
                    params["offset"] = offset
                updates = call("getUpdates", params, client=http, timeout=POLL_TIMEOUT + 10)
                backoff = 1
            except TelegramError as exc:
                log.warning("getUpdates failed: %s; retrying in %ss", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            for update in updates:
                # Advance first: a bad update must not be retried forever.
                offset = update["update_id"] + 1
                try:
                    for reply in handlers.handle(update):
                        result = send_message(reply.chat_id, reply.text, client=http)
                        if not result.ok:
                            log.warning("Reply to %s failed: %s", reply.chat_id, result.error)
                except Exception:
                    log.exception("Failed to handle update %s", update.get("update_id"))


if __name__ == "__main__":
    run()
