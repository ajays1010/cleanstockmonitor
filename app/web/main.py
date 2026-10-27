"""Web app entry point. Run with `uvicorn app.web.main:app`."""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.web.deps import AdminRequired, LoginRequired, login_url
from app.web.routes import auth, pages, telegram, watchlist

HERE = Path(__file__).parent


def create_app() -> FastAPI:
    setup_logging()
    settings = get_settings()
    settings.check_production()

    app = FastAPI(
        title="StockMonitor",
        docs_url=None if settings.is_production else "/docs",
        redoc_url=None,
        openapi_url=None if settings.is_production else "/openapi.json",
    )
    # The cookie is signed, not encrypted: it holds only the user id and short-lived login state.
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret.get_secret_value() or "dev-only-session-secret",
        session_cookie="sm_session",
        https_only=settings.is_production,
        same_site="lax",
        max_age=60 * 60 * 24 * 30,
    )
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")

    @app.exception_handler(LoginRequired)
    def to_login(request: Request, exc: LoginRequired):
        return RedirectResponse(login_url(exc.next_path), status_code=303)

    @app.exception_handler(AdminRequired)
    def not_admin(request: Request, exc: AdminRequired):
        return HTMLResponse("Admins only.", status_code=403)

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    app.include_router(pages.router)
    app.include_router(auth.router)
    app.include_router(watchlist.router)
    app.include_router(telegram.router)
    return app


app = create_app()
