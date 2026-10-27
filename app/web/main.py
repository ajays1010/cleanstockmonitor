"""Web app entry point. Run with `uvicorn app.web.main:app`."""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import get_settings
from app.core.logging import setup_logging

HERE = Path(__file__).parent
templates = Jinja2Templates(directory=HERE / "templates")


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
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret.get_secret_value() or "dev-only-session-secret",
        https_only=settings.is_production,
        same_site="lax",
        max_age=60 * 60 * 24 * 30,
    )
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        return templates.TemplateResponse(request, "index.html", {"user": request.session.get("user")})

    return app


app = create_app()
