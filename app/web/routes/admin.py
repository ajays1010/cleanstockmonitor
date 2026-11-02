"""Admin page: Fyers connection and the latest run of each background job."""

import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.core import fyers_link
from app.core.fyers_tokens import TokenStore
from app.core.profiles import Profile
from app.core.timeutil import IST, now_ist
from app.sources.fyers import FyersClient, FyersError
from app.web.deps import (
    AdminRequired,
    LoginRequired,
    get_fyers,
    get_job_runs,
    get_tokens,
    optional_user,
    require_admin,
)
from app.web.templating import templates

router = APIRouter(prefix="/admin")

AdminDep = Annotated[Profile, Depends(require_admin)]
FyersDep = Annotated[FyersClient, Depends(get_fyers)]
TokensDep = Annotated[TokenStore, Depends(get_tokens)]

STATE_KEY = "fyers_state"
LINK_KEY = "fyers_via_link"  # login started from the reminder's signed link


def _page(request: Request, user, fyers, tokens, runs, message=None, error=None, status_code=200):
    now = datetime.now(UTC)
    t = tokens.get()
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "user": user,
            "fyers_configured": fyers.configured,
            "redirect_uri": fyers.redirect_uri,
            "tokens": t,
            "token_live": bool(t and t.access_expires_at > now),
            "fmt": lambda d: d.astimezone(IST).strftime("%d %b %Y, %H:%M") if d else "—",
            "runs": runs(),
            "message": message,
            "error": error,
        },
        status_code=status_code,
    )


@router.get("", response_class=HTMLResponse)
def admin_home(
    request: Request,
    user: AdminDep,
    fyers: FyersDep,
    tokens: TokensDep,
    runs: Annotated[Callable[[], list[dict]], Depends(get_job_runs)],
    msg: str | None = None,
):
    return _page(request, user, fyers, tokens, runs, message=msg)


@router.get("/fyers/connect")
def fyers_connect(request: Request, user: AdminDep, fyers: FyersDep):
    state = secrets.token_urlsafe(16)
    request.session[STATE_KEY] = state
    return RedirectResponse(fyers.login_url(state), status_code=303)


def _link_page(request: Request, error: str | None = None):
    return templates.TemplateResponse(
        request,
        "fyers_link.html",
        {"user": None, "ok": error is None, "error": error},
        status_code=200 if error is None else 400,
    )


@router.get("/fyers/start", response_class=HTMLResponse)
def fyers_start(request: Request, fyers: FyersDep, t: str = ""):
    """The morning reminder's link: no site sign-in, straight to Fyers' login."""
    if not fyers.configured or not fyers_link.check_token(t, now_ist().date()):
        return _link_page(request, "This link has expired; each reminder's link works only on its day.")
    state = secrets.token_urlsafe(16)
    request.session[STATE_KEY] = state
    request.session[LINK_KEY] = True
    return RedirectResponse(fyers.login_url(state), status_code=303)


@router.get("/fyers/callback", response_class=HTMLResponse)
def fyers_callback(
    request: Request,
    user: Annotated[Profile | None, Depends(optional_user)],
    fyers: FyersDep,
    tokens: TokensDep,
    runs: Annotated[Callable[[], list[dict]], Depends(get_job_runs)],
    auth_code: str | None = None,
    code: str | None = None,
    state: str | None = None,
):
    expected = request.session.pop(STATE_KEY, None)
    via_link = request.session.pop(LINK_KEY, False)
    if not (user and user.is_admin):
        if not via_link:
            raise AdminRequired() if user else LoginRequired(request.url.path)
        # Started from the reminder's link in a browser not signed in here.
        if not expected or state != expected or not auth_code:
            return _link_page(request, "Fyers login was cancelled or expired.")
        try:
            tokens.save(fyers.exchange_code(auth_code, datetime.now(UTC)), datetime.now(UTC))
        except FyersError as exc:
            return _link_page(request, f"Fyers refused the login: {exc}")
        return _link_page(request)
    # Fyers sends the auth code as auth_code; `code` there is a status number.
    if not expected or state != expected or not auth_code:
        return _page(
            request,
            user,
            fyers,
            tokens,
            runs,
            error="Fyers login was cancelled or expired. Try again.",
            status_code=400,
        )
    now = datetime.now(UTC)
    try:
        tokens.save(fyers.exchange_code(auth_code, now), now)
    except FyersError as exc:
        return _page(
            request, user, fyers, tokens, runs, error=f"Fyers refused the login: {exc}", status_code=400
        )
    return RedirectResponse("/admin?msg=Fyers+connected.", status_code=303)
