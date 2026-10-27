"""Sign-in pages: Google (PKCE redirect) or a 6-digit code by email."""

import logging
import re
from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.core.config import get_settings
from app.core.profiles import Profile, ProfileStore
from app.core.supabase_auth import AuthError, AuthUser, SupabaseAuth, new_code_verifier
from app.web.deps import SESSION_USER_KEY, get_auth, get_profiles, optional_user, safe_next
from app.web.templating import templates

log = logging.getLogger(__name__)
router = APIRouter()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
CODE_RE = re.compile(r"^\d{6}$")

AuthDep = Annotated[SupabaseAuth, Depends(get_auth)]
ProfilesDep = Annotated[ProfileStore, Depends(get_profiles)]


def _start_session(request: Request, user: AuthUser, profiles: ProfileStore) -> RedirectResponse:
    profiles.ensure(user)
    next_path = safe_next(request.session.get("login_next"))
    request.session.clear()  # new session on login, so nothing from before carries over
    request.session[SESSION_USER_KEY] = user.id
    return RedirectResponse(next_path, status_code=303)


def _login_page(request: Request, error: str | None = None, status_code: int = 200) -> HTMLResponse:
    return templates.TemplateResponse(request, "login.html", {"error": error}, status_code=status_code)


@router.get("/login", response_class=HTMLResponse)
def login(request: Request, user: Annotated[Profile | None, Depends(optional_user)], next: str | None = None):
    if user:
        return RedirectResponse(safe_next(next), status_code=303)
    request.session["login_next"] = safe_next(next)
    return _login_page(request)


@router.get("/auth/google")
def google_start(request: Request, auth: AuthDep):
    verifier = new_code_verifier()
    request.session["pkce_verifier"] = verifier
    redirect_to = get_settings().app_base_url.rstrip("/") + "/auth/callback"
    return RedirectResponse(auth.authorize_url("google", redirect_to, verifier), status_code=303)


@router.get("/auth/callback")
def google_callback(
    request: Request,
    auth: AuthDep,
    profiles: ProfilesDep,
    code: str | None = None,
    error_description: str | None = None,
):
    verifier = request.session.pop("pkce_verifier", None)
    if not code or not verifier:
        message = error_description or "Google sign-in was cancelled or expired. Please try again."
        return _login_page(request, message, status_code=400)
    try:
        user = auth.exchange_code(code, verifier)
    except AuthError as exc:
        return _login_page(request, str(exc), status_code=400)
    return _start_session(request, user, profiles)


@router.post("/auth/email", response_class=HTMLResponse)
def email_send_code(request: Request, auth: AuthDep, email: Annotated[str, Form()]):
    email = email.strip().lower()
    if not EMAIL_RE.match(email) or len(email) > 254:
        return _login_page(request, "Enter a valid email address.", status_code=400)
    try:
        auth.send_email_code(email)
    except AuthError as exc:
        return _login_page(request, str(exc), status_code=400)
    request.session["login_email"] = email
    return templates.TemplateResponse(request, "login_code.html", {"email": email, "error": None})


@router.post("/auth/email/verify", response_class=HTMLResponse)
def email_verify_code(request: Request, auth: AuthDep, profiles: ProfilesDep, code: Annotated[str, Form()]):
    email = request.session.get("login_email")
    if not email:
        return _login_page(request, "Your sign-in expired. Enter your email again.", status_code=400)
    code = code.strip().replace(" ", "")
    error = None if CODE_RE.match(code) else "Enter the 6-digit code from the email."
    if error is None:
        try:
            user = auth.verify_email_code(email, code)
        except AuthError as exc:
            error = str(exc)
        else:
            return _start_session(request, user, profiles)
    return templates.TemplateResponse(
        request, "login_code.html", {"email": email, "error": error}, status_code=400
    )


@router.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=303)
