"""Signed-in pages. Watchlist and Telegram linking land here in the next steps."""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.core.profiles import Profile
from app.web.deps import optional_user, require_user
from app.web.templating import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def home(request: Request, user: Annotated[Profile | None, Depends(optional_user)]):
    if user:
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse(request, "index.html", {"user": None})


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, user: Annotated[Profile, Depends(require_user)]):
    return templates.TemplateResponse(request, "dashboard.html", {"user": user})
