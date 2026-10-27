"""Watchlist: search scrips, add and remove them, set alerts per scrip, and show prices.

Every change returns the re-rendered watchlist fragment for HTMX to swap in. Prices come
from price_snapshots; /watchlist/prices re-sends only the price lines, so a settings form
someone is filling in is never replaced.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse

from app.core.categories import CATEGORIES
from app.core.prices import PriceStore
from app.core.profiles import Profile
from app.core.timeutil import is_market_open, now_ist
from app.core.watchlist import MAX_WATCHLIST, WatchlistError, WatchlistStore, parse_settings
from app.web.deps import get_prices, get_watchlist, require_htmx, require_user
from app.web.templating import templates

router = APIRouter(prefix="/watchlist")

UserDep = Annotated[Profile, Depends(require_user)]
StoreDep = Annotated[WatchlistStore, Depends(get_watchlist)]
PricesDep = Annotated[PriceStore, Depends(get_prices)]


def watchlist_context(store: WatchlistStore, prices: PriceStore, user: Profile) -> dict:
    items = store.list(user.id)
    return {
        "items": items,
        "prices": prices.views([i.scrip.id for i in items]),
        # Prices only change during the session; outside it the page does not poll.
        "live_prices": bool(items) and is_market_open(now_ist(), set()),
        "categories": CATEGORIES,
        "max_items": MAX_WATCHLIST,
        "message": None,
        "error": None,
        "error_item": None,
    }


def render_watchlist(
    request: Request,
    store: WatchlistStore,
    prices: PriceStore,
    user: Profile,
    message: str | None = None,
    error: str | None = None,
    error_item: int | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    context = watchlist_context(store, prices, user)
    context.update(message=message, error=error, error_item=error_item)
    return templates.TemplateResponse(request, "partials/watchlist.html", context, status_code=status_code)


@router.get("/search", response_class=HTMLResponse)
def search(request: Request, user: UserDep, store: StoreDep, q: str = ""):
    results = store.search(q) if len(q.strip()) >= 2 else []
    watched = {i.scrip.id for i in store.list(user.id)} if results else set()
    return templates.TemplateResponse(
        request, "partials/search_results.html", {"q": q.strip(), "results": results, "watched": watched}
    )


@router.get("/prices", response_class=HTMLResponse)
def price_lines(request: Request, user: UserDep, store: StoreDep, prices: PricesDep):
    """Out-of-band swaps for each card's price line, plus the poller itself."""
    return templates.TemplateResponse(
        request, "partials/price_lines.html", watchlist_context(store, prices, user)
    )


@router.post("/add", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def add(
    request: Request, user: UserDep, store: StoreDep, prices: PricesDep, scrip_id: Annotated[int, Form()]
):
    try:
        store.add(user.id, scrip_id)
    except WatchlistError as exc:
        return render_watchlist(request, store, prices, user, error=str(exc), status_code=400)
    prices.refresh(scrip_id)  # one quote, so the new card shows a price straight away
    return render_watchlist(request, store, prices, user, message="Added to your watchlist.")


@router.post("/{item_id}/remove", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def remove(request: Request, user: UserDep, store: StoreDep, prices: PricesDep, item_id: int):
    store.remove(user.id, item_id)
    return render_watchlist(request, store, prices, user, message="Removed.")


@router.post("/{item_id}/settings", response_class=HTMLResponse, dependencies=[Depends(require_htmx)])
def update_settings(
    request: Request,
    user: UserDep,
    store: StoreDep,
    prices: PricesDep,
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
        return render_watchlist(
            request, store, prices, user, error=str(exc), error_item=item_id, status_code=400
        )
    return render_watchlist(request, store, prices, user, message="Alert settings saved.")
