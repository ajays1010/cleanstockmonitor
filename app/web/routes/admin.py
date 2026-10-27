"""Admin page: Fyers connection and the latest run of each background job."""

import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.core.fyers_tokens import TokenStore
from app.core.profiles import Profile
from app.core.timeutil import IST
from app.sources.fyers import FyersClient, FyersError
from app.web.deps import get_fyers, get_job_runs, get_tokens, require_admin
from app.web.templating import templates

router = APIRouter(prefix="/admin")

AdminDep = Annotated[Profile, Depends(require_admin)]
FyersDep = Annotated[FyersClient, Depends(get_fyers)]
TokensDep = Annotated[TokenStore, Depends(get_tokens)]

STATE_KEY = "fyers_state"


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


@router.get("/fyers/callback", response_class=HTMLResponse)
def fyers_callback(
    request: Request,
    user: AdminDep,
    fyers: FyersDep,
    tokens: TokensDep,
    runs: Annotated[Callable[[], list[dict]], Depends(get_job_runs)],
    auth_code: str | None = None,
    code: str | None = None,
    state: str | None = None,
):
    expected = request.session.pop(STATE_KEY, None)
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
