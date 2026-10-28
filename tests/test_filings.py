import base64
import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from app.core import alerts, filings, gemini, telegram
from app.core.telegram import SendResult
from app.core.timeutil import IST
from app.sources.bse import Announcement, BseError, download_pdf

NOW = datetime(2026, 10, 27, 14, 0, tzinfo=IST)
PDF = b"%PDF-1.7 fake filing"
SUMMARY = {"points": ["Revenue ₹412 Cr, +18% YoY", "PAT ₹56 Cr, +31% YoY"], "tone": "positive"}


def announcement(category="results", summary=None, title="Financial Results Q2"):
    a = Announcement(
        "n1",
        "533096",
        "Adani Power Ltd",
        title,
        "Result",
        "Financial Results",
        NOW - timedelta(minutes=10),
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/abc.pdf",
        category,
    )
    event = alerts.announcement_event(a, 7, "ADANIPOWER")
    event["id"] = 1
    if summary:
        event["payload"]["summary"] = summary
    return event


# ---- rendering


def test_render_shows_summary_and_tone():
    text = alerts.render(announcement(summary=SUMMARY))
    assert "🤖 <b>AI summary</b> (not advice)" in text
    assert "• Revenue ₹412 Cr, +18% YoY" in text and "Tone: 🟢 Positive" in text
    assert "Open filing (PDF)" in text


def test_caption_has_no_link_and_fits_telegram_limit():
    event = announcement(summary=SUMMARY)
    text = alerts.caption(event)
    assert "Open filing" not in text and "• PAT ₹56 Cr" in text

    long = announcement(summary={"points": ["x" * 190] * 4, "tone": "negative"}, title="T" * 500)
    text = alerts.caption(long)
    assert alerts._visible_len(text) <= alerts.MAX_CAPTION
    assert text.count("• ") < 4  # points dropped from the end to fit


def test_caption_without_summary():
    text = alerts.caption(announcement())
    assert "AI summary" not in text and "Financial Results Q2" in text


# ---- BSE PDF download


def pdf_client(content=PDF, status=200):
    return httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(status, content=content)))


def test_download_pdf():
    assert download_pdf("https://x/a.pdf", pdf_client()) == PDF
    with pytest.raises(BseError, match="Not a PDF"):
        download_pdf("https://x/a.pdf", pdf_client(b"<html>Access Denied</html>"))
    with pytest.raises(BseError, match="HTTP 404"):
        download_pdf("https://x/a.pdf", pdf_client(status=404))


def test_download_pdf_refuses_huge_files(monkeypatch):
    monkeypatch.setattr("app.sources.bse.MAX_PDF_BYTES", 10)
    with pytest.raises(BseError, match="PDF over"):
        download_pdf("https://x/a.pdf", pdf_client(PDF + b"x" * 100))


# ---- Gemini


@pytest.fixture
def gemini_key(monkeypatch):
    settings = SimpleNamespace(gemini_api_key=SecretStr("gk"), gemini_model="gemini-test")
    monkeypatch.setattr(gemini, "get_settings", lambda: settings)
    return settings


def gemini_reply(result, status=200):
    if status != 200:
        return httpx.Response(status, json={"error": {"message": result}})
    parts = [{"text": "thinking...", "thought": True}, {"text": json.dumps(result)}]
    return httpx.Response(200, json={"candidates": [{"content": {"parts": parts}}]})


def test_summarise_sends_pdf_inline_and_parses(gemini_key):
    seen = {}

    def handler(req):
        seen["url"], seen["key"], seen["body"] = (
            str(req.url),
            req.headers["x-goog-api-key"],
            json.loads(req.content),
        )
        return gemini_reply({"points": ["  Revenue up   18% ", "", "PAT up"], "tone": "positive"})

    s = gemini.summarise(PDF, "Adani Power", "Results", httpx.Client(transport=httpx.MockTransport(handler)))
    assert s == gemini.Summary(("Revenue up 18%", "PAT up"), "positive")
    assert seen["url"].endswith("/models/gemini-test:generateContent") and seen["key"] == "gk"
    inline = seen["body"]["contents"][0]["parts"][0]["inline_data"]
    assert inline["mime_type"] == "application/pdf" and base64.b64decode(inline["data"]) == PDF
    assert seen["body"]["generationConfig"]["responseMimeType"] == "application/json"


def test_summarise_retries_once_when_busy(gemini_key, monkeypatch):
    monkeypatch.setattr(gemini, "RETRY_DELAY", 0)
    replies = [gemini_reply("high demand", 503), gemini_reply({"points": ["ok"], "tone": "neutral"})]
    client = httpx.Client(transport=httpx.MockTransport(lambda r: replies.pop(0)))
    assert gemini.summarise(PDF, "X", "Y", client).points == ("ok",)


def test_summarise_errors(gemini_key, monkeypatch):
    monkeypatch.setattr(gemini, "RETRY_DELAY", 0)

    def run(reply):
        client = httpx.Client(transport=httpx.MockTransport(lambda r: reply))
        return gemini.summarise(PDF, "X", "Y", client)

    with pytest.raises(gemini.GeminiError, match="HTTP 429: quota"):
        run(gemini_reply("quota", 429))
    with pytest.raises(gemini.GeminiError, match="no points"):
        run(gemini_reply({"points": [], "tone": "neutral"}))
    with pytest.raises(gemini.GeminiError, match="SAFETY"):
        run(httpx.Response(200, json={"promptFeedback": {"blockReason": "SAFETY"}}))


def test_unknown_tone_is_neutral_and_points_capped():
    data = gemini_reply({"points": [f"p{i}" for i in range(6)], "tone": "bullish"}).json()
    s = gemini.parse_response(data)
    assert s.tone == "neutral" and len(s.points) == gemini.MAX_POINTS


def test_control_characters_are_stripped():
    data = gemini_reply({"points": ["PAT up to \x08₹7,019 Cr\nnet"], "tone": "positive"}).json()
    assert gemini.parse_response(data).points == ("PAT up to ₹7,019 Cr net",)


# ---- Telegram documents


@pytest.fixture
def tg(monkeypatch):
    monkeypatch.setattr(telegram, "_token", lambda: "T")
    calls = []

    def client(reply):
        def handler(req):
            calls.append(req)
            return reply

        return httpx.Client(transport=httpx.MockTransport(handler))

    return calls, client


def test_send_document_uploads_then_resends_by_file_id(tg):
    calls, client = tg
    ok = httpx.Response(200, json={"ok": True, "result": {"document": {"file_id": "F1"}}})
    r = telegram.send_document(5, "<b>cap</b>", pdf=PDF, filename="a.pdf", client=client(ok))
    assert r == SendResult(ok=True, file_id="F1")
    body = calls[-1].read()
    assert calls[-1].url.path.endswith("/sendDocument") and b'filename="a.pdf"' in body and PDF in body
    assert b"<b>cap</b>" in body and b"HTML" in body

    telegram.send_document(5, "cap", file_id="F1", client=client(ok))
    assert json.loads(calls[-1].content)["document"] == "F1"


# ---- prepare and deliver


@pytest.fixture
def stubs(monkeypatch):
    state = {"pdf": PDF, "summary": gemini.Summary(("Revenue up",), "positive"), "configured": True}
    calls = {"summarise": 0, "documents": [], "messages": []}

    def fake_download(url, http):
        if isinstance(state["pdf"], Exception):
            raise state["pdf"]
        return state["pdf"]

    def fake_summarise(pdf, company, headline):
        calls["summarise"] += 1
        if isinstance(state["summary"], Exception):
            raise state["summary"]
        return state["summary"]

    def fake_document(chat_id, caption, *, pdf=None, file_id=None, filename=None):
        calls["documents"].append((chat_id, bool(pdf), file_id, filename))
        return state.get("doc_result") or SendResult(ok=True, file_id="F1")

    def fake_message(chat_id, text):
        calls["messages"].append((chat_id, text))
        return SendResult(ok=True)

    monkeypatch.setattr(filings, "download_pdf", fake_download)
    monkeypatch.setattr(filings.gemini, "summarise", fake_summarise)
    monkeypatch.setattr(filings.gemini, "configured", lambda: state["configured"])
    monkeypatch.setattr(filings, "send_document", fake_document)
    monkeypatch.setattr(filings, "send_message", fake_message)
    return state, calls


def test_prepare_summarises_important_filings_once(stubs):
    state, calls = stubs
    event = announcement()
    assert filings.prepare(event, None) == PDF
    assert event["payload"]["summary"] == {"points": ["Revenue up"], "tone": "positive"}
    filings.prepare(event, None)
    assert calls["summarise"] == 1


def test_prepare_skips_routine_filings_and_missing_key(stubs):
    state, calls = stubs
    routine = announcement(category="other")
    assert filings.prepare(routine, None) == PDF and "summary" not in routine["payload"]
    state["configured"] = False
    assert filings.prepare(announcement(), None) == PDF
    assert calls["summarise"] == 0


def test_prepare_survives_bse_and_gemini_failures(stubs):
    state, calls = stubs
    state["summary"] = gemini.GeminiError("HTTP 503")
    event = announcement()
    assert filings.prepare(event, None) == PDF
    assert event["payload"]["summary_error"] == "HTTP 503" and "summary" not in event["payload"]
    state["pdf"] = BseError("HTTP 403")
    assert filings.prepare(announcement(), None) is None


def test_deliver_uploads_once_then_reuses_file_id(stubs):
    state, calls = stubs
    event = announcement()
    assert filings.deliver(1, event, PDF).ok
    assert filings.deliver(2, event, PDF).ok
    assert calls["documents"] == [
        (1, True, None, "ADANIPOWER_abc.pdf"),
        (2, False, "F1", "ADANIPOWER_abc.pdf"),
    ]
    assert event["payload"]["tg_file_id"] == "F1" and calls["messages"] == []


def test_deliver_falls_back_to_text(stubs):
    state, calls = stubs
    filings.deliver(1, announcement(), None)  # no PDF downloaded
    state["doc_result"] = SendResult(ok=False, error="Bad Request: file is too big")
    filings.deliver(2, announcement(), PDF)
    assert [c for c, _ in calls["messages"]] == [1, 2]
    assert "Open filing (PDF)" in calls["messages"][0][1]


def test_deliver_keeps_document_when_rate_limited(stubs):
    state, calls = stubs
    state["doc_result"] = SendResult(ok=False, error="Too Many Requests", retry_after=3)
    assert filings.deliver(1, announcement(), PDF).retry_after == 3
    assert calls["messages"] == []


def test_price_alerts_still_go_as_text(stubs):
    state, calls = stubs
    event = {"id": 2, "kind": "daily_summary", "title": "t", "payload": {"text": "hello"}}
    filings.deliver(1, event, None)
    assert calls["messages"] == [(1, "hello")] and calls["documents"] == []
