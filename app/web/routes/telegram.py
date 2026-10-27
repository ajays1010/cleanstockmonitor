"""Telegram panel on the dashboard: connect a chat, send a test message, disconnect.

Connecting creates a one-time code and shows two bot links (private chat, or add to a
group). The panel then polls until the bot reports the new chat, for up to 10 minutes.
"""

import time
from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from app.core.profiles import Profile
from app.core.telegram import SendResult, esc
from app.core.telegram_links import CODE_TTL, TelegramLinkStore
from app.web.deps import get_bot_username, get_sender, get_telegram_links, require_htmx, require_user
from app.web.templating import templates

router = APIRouter(prefix="/telegram")

UserDep = Annotated[Profile, Depends(require_user)]
LinksDep = Annotated[TelegramLinkStore, Depends(get_telegram_links)]
BotDep = Annotated[str | None, Depends(get_bot_username)]

PENDING_KEY = "tg_pending"  # {"code", "until", "chats"} while a connect is in progress


def panel_context(request: Request, user: Profile, links: TelegramLinkStore, bot: str | None) -> dict:
    chats = links.chats_for_user(user.id)
    pending = request.session.get(PENDING_KEY)
    message = None
    if pending and (time.time() > pending["until"] or len(chats) > pending["chats"]):
        if len(chats) > pending["chats"]:
            message = "Connected. Alerts will go to this chat."
        request.session.pop(PENDING_KEY, None)
        pending = None
    return {"bot": bot, "chats": chats, "pending": pending, "tg_message": message, "tg_error": None}


def render_panel(request: Request, context: dict, status_code: int = 200) -> HTMLResponse:
    return templates.TemplateResponse(request, "partials/telegram.html", context, status_code=status_code)


@router.get("/panel", response_class=HTMLResponse)
def panel(request: Request, user: UserDep, links: LinksDep, bot: BotDep):
    return render_panel(request, panel_context(request, user, links, bot))


@router.post("/connect", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def connect(request: Request, user: UserDep, links: LinksDep, bot: BotDep):
    if bot:
        request.session[PENDING_KEY] = {
            "code": links.create_code(user.id),
            "until": time.time() + CODE_TTL.total_seconds(),
            "chats": len(links.chats_for_user(user.id)),
        }
    return render_panel(request, panel_context(request, user, links, bot))


@router.post("/cancel", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def cancel(request: Request, user: UserDep, links: LinksDep, bot: BotDep):
    request.session.pop(PENDING_KEY, None)
    return render_panel(request, panel_context(request, user, links, bot))


@router.post("/{link_id}/test", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def test_message(
    request: Request,
    user: UserDep,
    links: LinksDep,
    bot: BotDep,
    send: Annotated[Callable[[int, str], SendResult], Depends(get_sender)],
    link_id: int,
):
    context = panel_context(request, user, links, bot)
    chat = links.get_for_user(user.id, link_id)
    if chat is None:
        context["tg_error"] = "That chat is no longer connected."
        return render_panel(request, context, status_code=400)
    name = user.display_name or user.email or "you"
    result = send(
        chat.chat_id, f"✅ Test message from StockMonitor for {esc(name)}. Alerts will arrive here."
    )
    if result.ok:
        context["tg_message"] = f"Test message sent to {chat.label or 'the chat'}."
    else:
        context["tg_error"] = f"Telegram refused the message: {result.error}"
    return render_panel(request, context, status_code=200 if result.ok else 400)


@router.post("/{link_id}/remove", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def remove(request: Request, user: UserDep, links: LinksDep, bot: BotDep, link_id: int):
    links.remove_for_user(user.id, link_id)
    context = panel_context(request, user, links, bot)
    context["tg_message"] = "Disconnected."
    return render_panel(request, context)
