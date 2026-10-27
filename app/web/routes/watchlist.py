"""Watchlist: search scrips, add and remove them, and set alerts per scrip.

Every change returns the re-rendered watchlist fragment for HTMX to swap in.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse

from app.core.categories import CATEGORIES
from app.core.profiles import Profile
from app.core.watchlist import MAX_WATCHLIST, WatchlistError, WatchlistStore, parse_settings
from app.web.deps import get_watchlist, require_htmx, require_user
from app.web.templating import templates

router = APIRouter(prefix="/watchlist")

UserDep = Annotated[Profile, Depends(require_user)]
StoreDep = Annotated[WatchlistStore, Depends(get_watchlist)]


def render_watchlist(
    request: Request,
    store: WatchlistStore,
    user: Profile,
    message: str | None = None,
    error: str | None = None,
    error_item: int | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "partials/watchlist.html",
        {
            "items": store.list(user.id),
            "categories": CATEGORIES,
            "max_items": MAX_WATCHLIST,
            "message": message,
            "error": error,
            "error_item": error_item,
        },
        status_code=status_code,
    )


@router.get("/search", response_class=HTMLResponse)
def search(request: Request, user: UserDep, store: StoreDep, q: str = ""):
    results = store.search(q) if len(q.strip()) >= 2 else []
    watched = {i.scrip.id for i in store.list(user.id)} if results else set()
    return templates.TemplateResponse(
        request, "partials/search_results.html", {"q": q.strip(), "results": results, "watched": watched}
    )


@router.post("/add", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def add(request: Request, user: UserDep, store: StoreDep, scrip_id: Annotated[int, Form()]):
    try:
        store.add(user.id, scrip_id)
    except WatchlistError as exc:
        return render_watchlist(request, store, user, error=str(exc), status_code=400)
    return render_watchlist(request, store, user, message="Added to your watchlist.")


@router.post("/{item_id}/remove", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def remove(request: Request, user: UserDep, store: StoreDep, item_id: int):
    store.remove(user.id, item_id)
    return render_watchlist(request, store, user, message="Removed.")


@router.post("/{item_id}/settings", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def update_settings(
    request: Request,
    user: UserDep,
    store: StoreDep,
    item_id: int,
    spike_pct: Annotated[str, Form()] = "",
    price_above: Annotated[str, Form()] = "",
    price_below: Annotated[str, Form()] = "",
    categories: Annotated[list[str] | None, Form()] = None,
):
    try:
        settings = parse_settings(spike_pct, price_above, price_below, categories or [])
        store.update(user.id, item_id, settings)
    except WatchlistError as exc:
        return render_watchlist(request, store, user, error=str(exc), error_item=item_id, status_code=400)
    return render_watchlist(request, store, user, message="Alert settings saved.")
