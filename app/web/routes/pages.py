"""Home page and the signed-in dashboard (Telegram panel and watchlist)."""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.core.categories import CATEGORIES
from app.core.profiles import Profile
from app.core.telegram_links import TelegramLinkStore
from app.core.watchlist import MAX_WATCHLIST, WatchlistStore
from app.web.deps import get_bot_username, get_telegram_links, get_watchlist, optional_user, require_user
from app.web.routes.telegram import panel_context
from app.web.templating import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def home(request: Request, user: Annotated[Profile | None, Depends(optional_user)]):
    if user:
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse(request, "index.html", {"user": None})


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    user: Annotated[Profile, Depends(require_user)],
    store: Annotated[WatchlistStore, Depends(get_watchlist)],
    links: Annotated[TelegramLinkStore, Depends(get_telegram_links)],
    bot: Annotated[str | None, Depends(get_bot_username)],
):
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "user": user,
            "items": store.list(user.id),
            "categories": CATEGORIES,
            "max_items": MAX_WATCHLIST,
            "message": None,
            "error": None,
            "error_item": None,
            **panel_context(request, user, links, bot),
        },
    )
