"""Scheduled jobs: poll BSE and prices, fan events out to Telegram, retry, summarise, clean up.

Pollers only write `events` (unique per source and source_id, so re-polling is harmless).
Fan-out turns each new event into `deliveries` rows (unique per event and chat) and sends
only the rows it just inserted, so a chat never gets the same event twice.
"""

import logging
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

import httpx

from app.core import alerts, filings
from app.core.db import get_db
from app.core.quote_source import fetch_quotes, snapshot_row
from app.core.timeutil import IST, MARKET_OPEN, now_ist
from app.jobs.runner import tracked
from app.sources import nse
from app.sources.bse import BseError, fetch_announcements, parse_rows

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
ANNOUNCEMENT_HOURS = range(7, 24)  # IST; filings arrive from early morning to late evening
PRICE_WINDOW_END = (15, 40)  # keep polling a little past 15:30 to catch the closing price
# Telegram errors that will not fix themselves; stop retrying those deliveries.
PERMANENT_ERRORS = ("blocked by the user", "chat not found", "kicked", "deactivated", "not enough rights")

WATCH_COLUMNS = (
    "user_id, created_at, spike_pct, price_above, price_below, categories, "
    "scrips(id, bse_code, name, nse_symbol, yahoo_symbol, fyers_symbol)"
)


def _watchlist() -> list[dict]:
    return get_db().table("watchlist").select(WATCH_COLUMNS).execute().data


def _distinct_scrips(rows: list[dict]) -> dict[int, dict]:
    return {r["scrips"]["id"]: r["scrips"] for r in rows}


def watched_bse_codes() -> list[str]:
    return sorted({s["bse_code"] for s in _distinct_scrips(_watchlist()).values()})


def _insert_events(events: list[dict]) -> int:
    if not events:
        return 0
    inserted = (
        get_db()
        .table("events")
        .upsert(events, on_conflict="source,source_id", ignore_duplicates=True)
        .execute()
        .data
    )
    return len(inserted)


def _holidays() -> set[date]:
    rows = get_db().table("app_settings").select("value").eq("key", "market_holidays").execute().data
    return {date.fromisoformat(d) for d in (rows[0]["value"] if rows else [])}


def _store_bse(
    scrips: dict[int, dict], fetched: dict[str, list], errors: int, baseline: bool = False
) -> dict:
    """Save BSE announcements fetched per BSE code. A baseline run records them as already
    sent: used when switching back to BSE, so filings that went out from NSE are not repeated."""
    if errors and not fetched:
        raise RuntimeError(f"BSE failed for all {errors} scrips")
    events = [
        alerts.announcement_event(a, s["id"], s.get("nse_symbol"))
        for s in scrips.values()
        for a in fetched.get(s["bse_code"], [])
    ]
    if baseline:
        stamp = datetime.now(UTC).isoformat()
        for e in events:
            e["fanned_out_at"] = stamp
    stats = {
        "scrips": len(fetched) + errors,
        "fetched": len(events),
        "new": _insert_events(events),
        "errors": errors,
    }
    return {**stats, "baseline": True} if baseline else stats


@tracked("poll_announcements")
def poll_announcements() -> dict:
    scrips = _distinct_scrips(_watchlist())
    fetched, errors = {}, 0
    with httpx.Client() as http:
        for scrip in scrips.values():
            try:
                fetched[scrip["bse_code"]] = fetch_announcements(scrip["bse_code"], http)
            except BseError as exc:
                errors += 1
                log.warning("BSE %s: %s", scrip["bse_code"], exc)
            time.sleep(0.3)  # be gentle with BSE
    return _store_bse(scrips, fetched, errors)


@tracked("poll_announcements")
def store_relayed_announcements(results: dict[str, list[dict] | None], baseline: bool = False) -> dict:
    """Announcements the home relay fetched from BSE: raw rows per BSE code, or None where
    BSE failed. Recorded as poll_announcements so the watchdog and admin page see them."""
    scrips = _distinct_scrips(_watchlist())
    fetched = {code: parse_rows(rows) for code, rows in results.items() if rows is not None}
    errors = sum(1 for rows in results.values() if rows is None)
    return _store_bse(scrips, fetched, errors, baseline)


def _has_events(source: str) -> bool:
    return bool(get_db().table("events").select("id").eq("source", source).limit(1).execute().data)


@tracked("poll_nse_announcements")
def poll_nse_announcements() -> dict:
    """NSE filings for watched scrips listed there; used while BSE is unreachable
    (ANNOUNCEMENT_SOURCE=nse). Scrips listed only on BSE are not covered."""
    scrips = _distinct_scrips(_watchlist())
    listed = [s for s in scrips.values() if s.get("nse_symbol")]
    # The first NSE poll only records what is there: those filings already went out from BSE.
    baseline = not _has_events("nse")
    events, errors = [], 0
    with nse.client() as http:
        for s in listed:
            sme = nse.is_sme(s.get("fyers_symbol"))
            try:
                for a in nse.fetch_announcements(s["nse_symbol"], s["bse_code"], http, sme=sme):
                    events.append(alerts.announcement_event(a, s["id"], s["nse_symbol"], source="nse"))
            except nse.NseError as exc:
                errors += 1
                log.warning("NSE %s: %s", s["nse_symbol"], exc)
            time.sleep(0.3)  # be gentle with NSE
    if errors and errors == len(listed):
        raise RuntimeError(f"NSE failed for all {errors} scrips")
    if baseline:
        stamp = datetime.now(UTC).isoformat()
        for e in events:
            e["fanned_out_at"] = stamp
    return {
        "scrips": len(listed),
        "bse_only": len(scrips) - len(listed),
        "fetched": len(events),
        "new": _insert_events(events),
        "errors": errors,
        "baseline": baseline,
    }


@tracked("poll_prices")
def poll_prices() -> dict:
    now = now_ist()
    end = now.replace(hour=PRICE_WINDOW_END[0], minute=PRICE_WINDOW_END[1], second=0, microsecond=0)
    if now.weekday() >= 5 or now.date() in _holidays() or not (MARKET_OPEN <= now.time() <= end.time()):
        return {"skipped": "market closed"}

    rows = _watchlist()
    scrips = _distinct_scrips(rows)
    fetched = fetch_quotes(list(scrips.values()))
    # A quote not stamped today means a holiday or no trade today: never alert on it.
    quotes = {i: q for i, q in fetched.items() if q.quoted_at.date() == now.date()}
    stale = len(fetched) - len(quotes)

    if quotes:
        get_db().table("price_snapshots").upsert(
            [snapshot_row(scrip_id, q) for scrip_id, q in quotes.items()], on_conflict="scrip_id"
        ).execute()

    events = []
    for row in rows:
        quote = quotes.get(row["scrips"]["id"])
        if quote:
            events += alerts.price_events(row["user_id"], row["scrips"], row, quote, now.date())
    return {
        "scrips": len(scrips),
        "quotes": len(quotes),
        "from_fyers": sum(1 for q in quotes.values() if q.source == "fyers"),
        "stale": stale,
        "new_alerts": _insert_events(events),
    }


def _active_chats() -> dict[str, list[int]]:
    rows = get_db().table("telegram_chats").select("user_id, chat_id").eq("is_active", True).execute().data
    by_user: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        by_user[r["user_id"]].append(r["chat_id"])
    return by_user


def _save_payload(event: dict) -> None:
    get_db().table("events").update({"payload": event["payload"]}).eq("id", event["id"]).execute()


def _send(delivery_id: int, chat_id: int, event: dict, attempts: int, pdf: bytes | None = None) -> bool:
    uploaded = (event.get("payload") or {}).get("tg_file_id")
    result = filings.deliver(chat_id, event, pdf)
    if (event.get("payload") or {}).get("tg_file_id") != uploaded:
        _save_payload(event)  # later chats and retries resend Telegram's copy
    db = get_db().table("deliveries")
    if result.ok:
        db.update(
            {
                "status": "sent",
                "attempts": attempts + 1,
                "sent_at": datetime.now(UTC).isoformat(),
                "last_error": None,
            }
        ).eq("id", delivery_id).execute()
        return True
    permanent = any(p in (result.error or "").lower() for p in PERMANENT_ERRORS)
    db.update(
        {
            "status": "failed",
            "attempts": MAX_ATTEMPTS if permanent else attempts + 1,
            "last_error": (result.error or "")[:500],
        }
    ).eq("id", delivery_id).execute()
    if result.retry_after:
        time.sleep(min(result.retry_after, 30))
    return False


@tracked("fan_out")
def fan_out() -> dict:
    db = get_db()
    events = db.table("events").select("*").is_("fanned_out_at", "null").order("id").limit(200).execute().data
    if not events:
        return {"events": 0}
    chats = _active_chats()
    watch_by_scrip: dict[int, list[dict]] = defaultdict(list)
    for w in db.table("watchlist").select("user_id, scrip_id, created_at, categories").execute().data:
        watch_by_scrip[w["scrip_id"]].append(w)

    now = now_ist()
    sent = failed = 0
    http = httpx.Client(timeout=60)
    try:
        for event in events:
            ok, bad = _fan_out_event(db, http, event, watch_by_scrip, chats, now)
            sent += ok
            failed += bad
    finally:
        http.close()
    return {"events": len(events), "sent": sent, "failed": failed}


def _fan_out_event(db, http, event, watch_by_scrip, chats, now) -> tuple[int, int]:
    """Create one event's deliveries and send them; returns (sent, failed)."""
    pairs = alerts.targets(event, watch_by_scrip.get(event["scrip_id"], []), chats, now)
    pdf = None
    new_rows = []
    if pairs:
        # Only filings someone will receive are downloaded and summarised.
        before = dict(event.get("payload") or {})
        pdf = filings.prepare(event, http)
        if event.get("payload") != before:
            _save_payload(event)
        new_rows = (
            db.table("deliveries")
            .upsert(
                [{"event_id": event["id"], "chat_id": c, "user_id": u} for c, u in pairs],
                on_conflict="event_id,chat_id",
                ignore_duplicates=True,
            )
            .execute()
            .data
        )
    db.table("events").update({"fanned_out_at": datetime.now(UTC).isoformat()}).eq(
        "id", event["id"]
    ).execute()
    sent = sum(_send(row["id"], row["chat_id"], event, 0, pdf) for row in new_rows)
    return sent, len(new_rows) - sent


@tracked("retry_failed")
def retry_failed() -> dict:
    cutoff = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    rows = (
        get_db()
        .table("deliveries")
        .select("id, chat_id, attempts, events(*)")
        .in_("status", ["pending", "failed"])
        .lt("attempts", MAX_ATTEMPTS)
        .lt("created_at", cutoff)
        .order("id")
        .limit(100)
        .execute()
        .data
    )
    sent = sum(_send(r["id"], r["chat_id"], r["events"], r["attempts"]) for r in rows)
    return {"retried": len(rows), "sent": sent}


@tracked("daily_summary")
def daily_summary() -> dict:
    today = now_ist().date()
    if today.weekday() >= 5 or today in _holidays():
        return {"skipped": "not a trading day"}
    db = get_db()
    start = datetime.combine(today, datetime.min.time(), tzinfo=IST)
    snapshots = {
        s["scrip_id"]: s
        for s in db.table("price_snapshots").select("*").gte("quoted_at", start.isoformat()).execute().data
    }
    if not snapshots:
        return {"skipped": "no prices today"}
    counts: dict[int, int] = defaultdict(int)
    for e in (
        db.table("events")
        .select("scrip_id")
        .eq("kind", "announcement")
        .gte("occurred_at", start.isoformat())
        .execute()
        .data
    ):
        counts[e["scrip_id"]] += 1

    by_user: dict[str, list[dict]] = defaultdict(list)
    for row in _watchlist():
        by_user[row["user_id"]].append({**row["scrips"], "quote": snapshots.get(row["scrips"]["id"])})
    chats = _active_chats()
    events = [
        {
            "kind": "daily_summary",
            "scrip_id": None,
            "source": "system",
            "source_id": f"summary:{user_id}:{today.isoformat()}",
            "title": f"Close summary {today.isoformat()}",
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": {"user_id": user_id, "text": alerts.summary_text(rows, counts, today)},
        }
        for user_id, rows in by_user.items()
        if chats.get(user_id)
    ]
    return {"users": len(events), "new": _insert_events(events)}


# Events (and their deliveries, by cascade) are kept a little longer than the BSE poll
# window (2 days): the stored row is what tells the poller a filing was already handled.
EVENT_RETENTION = timedelta(days=3)
JOB_RUN_RETENTION = timedelta(days=7)


@tracked("cleanup")
def cleanup() -> dict:
    db = get_db()
    now = datetime.now(UTC)
    db.table("telegram_link_codes").delete().lt("expires_at", now.isoformat()).execute()
    old_events = db.table("events").delete().lt("created_at", (now - EVENT_RETENTION).isoformat()).execute()
    old_runs = db.table("job_runs").delete().lt("started_at", (now - JOB_RUN_RETENTION).isoformat()).execute()
    return {"events_deleted": len(old_events.data), "job_runs_deleted": len(old_runs.data)}
