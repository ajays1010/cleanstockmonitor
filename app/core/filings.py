"""Announcement alerts with the filing attached: the PDF goes out as a Telegram document
with the alert as its caption, plus a Gemini summary for important categories.

Both are done once per event. The PDF is uploaded to Telegram once; other chats (and
retries) get Telegram's copy by the file_id kept in the event payload.
"""

import logging
from posixpath import basename
from urllib.parse import urlparse

import httpx

from app.core import alerts, gemini
from app.core.telegram import SendResult, send_document, send_message
from app.sources.bse import BseError, download_pdf

log = logging.getLogger(__name__)


def prepare(event: dict, http: httpx.Client) -> bytes | None:
    """Download an announcement's PDF and add the AI summary to its payload (once).
    Returns the PDF, or None when there is none to attach. Never raises: an alert must
    go out even when BSE or Gemini fail."""
    p = event.get("payload") or {}
    if event["kind"] != "announcement" or not p.get("pdf_url"):
        return None
    try:
        pdf = download_pdf(p["pdf_url"], http)
    except BseError as exc:
        log.warning("No PDF for event %s: %s", event.get("id"), exc)
        return None
    wants_summary = p.get("category") in gemini.SUMMARY_CATEGORIES and gemini.configured()
    if wants_summary and "summary" not in p and "summary_error" not in p:
        if len(pdf) > gemini.MAX_PDF_BYTES:
            p["summary_error"] = "PDF too large to summarise"
        else:
            try:
                p["summary"] = gemini.summarise(pdf, p.get("company"), event["title"]).as_payload()
            except gemini.GeminiError as exc:
                log.warning("No summary for event %s: %s", event.get("id"), exc)
                p["summary_error"] = str(exc)[:300]
        event["payload"] = p
    return pdf


def _filename(p: dict) -> str:
    name = basename(urlparse(p.get("pdf_url") or "").path) or "filing.pdf"
    tag = p.get("symbol") or p.get("bse_code")
    return f"{tag}_{name}" if tag else name


def deliver(chat_id: int, event: dict, pdf: bytes | None = None) -> SendResult:
    """Send an event to one chat. Announcements go as their PDF when it was downloaded
    (or uploaded before); everything else, and any failed document, goes as text."""
    p = event.get("payload") or {}
    file_id = p.get("tg_file_id")
    if event["kind"] == "announcement" and (file_id or pdf):
        result = send_document(
            chat_id,
            alerts.caption(event),
            pdf=None if file_id else pdf,
            file_id=file_id,
            filename=_filename(p),
        )
        if result.ok:
            if result.file_id and not file_id:
                p["tg_file_id"] = result.file_id
                event["payload"] = p
            return result
        if result.retry_after:  # rate-limited: retry later, still as a document
            return result
        log.warning("Sending event %s as a PDF failed (%s); sending text", event.get("id"), result.error)
    return send_message(chat_id, alerts.render(event))
