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

from app.core import alerts
from app.core.db import get_db
from app.core.telegram import send_message
from app.core.timeutil import IST, MARKET_OPEN, now_ist
from app.jobs.runner import tracked
from app.sources.bse import BseError, fetch_announcements
from app.sources.quotes import fetch_yahoo

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
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


@tracked("poll_announcements")
def poll_announcements() -> dict:
    scrips = _distinct_scrips(_watchlist())
    events, errors = [], 0
    with httpx.Client() as http:
        for scrip in scrips.values():
            try:
                for a in fetch_announcements(scrip["bse_code"], http):
                    events.append(alerts.announcement_event(a, scrip["id"], scrip.get("nse_symbol")))
            except BseError as exc:
                errors += 1
                log.warning("BSE %s: %s", scrip["bse_code"], exc)
            time.sleep(0.3)  # be gentle with BSE
    if errors and errors == len(scrips):
        raise RuntimeError(f"BSE failed for all {errors} scrips")
    return {"scrips": len(scrips), "fetched": len(events), "new": _insert_events(events), "errors": errors}


@tracked("poll_prices")
def poll_prices() -> dict:
    now = now_ist()
    end = now.replace(hour=PRICE_WINDOW_END[0], minute=PRICE_WINDOW_END[1], second=0, microsecond=0)
    if now.weekday() >= 5 or now.date() in _holidays() or not (MARKET_OPEN <= now.time() <= end.time()):
        return {"skipped": "market closed"}

    rows = _watchlist()
    scrips = _distinct_scrips(rows)
    quotes, stale = {}, 0
    with httpx.Client() as http:
        for scrip_id, scrip in scrips.items():
            symbol = scrip.get("yahoo_symbol")
            quote = fetch_yahoo(symbol, http) if symbol else None
            if quote is None:
                continue
            if quote.quoted_at.date() != now.date():  # holiday or no trade today
                stale += 1
                continue
            quotes[scrip_id] = quote

    if quotes:
        get_db().table("price_snapshots").upsert(
            [
                {
                    "scrip_id": scrip_id,
                    "ltp": str(q.ltp),
                    "prev_close": str(q.prev_close) if q.prev_close is not None else None,
                    "day_high": str(q.day_high) if q.day_high is not None else None,
                    "day_low": str(q.day_low) if q.day_low is not None else None,
                    "volume": q.volume,
                    "source": q.source,
                    "quoted_at": q.quoted_at.isoformat(),
                }
                for scrip_id, q in quotes.items()
            ],
            on_conflict="scrip_id",
        ).execute()

    events = []
    for row in rows:
        quote = quotes.get(row["scrips"]["id"])
        if quote:
            events += alerts.price_events(row["user_id"], row["scrips"], row, quote, now.date())
    return {
        "scrips": len(scrips),
        "quotes": len(quotes),
        "stale": stale,
        "new_alerts": _insert_events(events),
    }


def _active_chats() -> dict[str, list[int]]:
    rows = get_db().table("telegram_chats").select("user_id, chat_id").eq("is_active", True).execute().data
    by_user: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        by_user[r["user_id"]].append(r["chat_id"])
    return by_user


def _send(delivery_id: int, chat_id: int, event: dict, attempts: int) -> bool:
    result = send_message(chat_id, alerts.render(event))
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
    for event in events:
        pairs = alerts.targets(event, watch_by_scrip.get(event["scrip_id"], []), chats, now)
        new_rows = []
        if pairs:
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
        for row in new_rows:
            if _send(row["id"], row["chat_id"], event, 0):
                sent += 1
            else:
                failed += 1
    return {"events": len(events), "sent": sent, "failed": failed}


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


@tracked("cleanup")
def cleanup() -> dict:
    db = get_db()
    now = datetime.now(UTC)
    db.table("telegram_link_codes").delete().lt("expires_at", now.isoformat()).execute()
    old_events = (
        db.table("events").delete().lt("created_at", (now - timedelta(days=90)).isoformat()).execute()
    )
    old_runs = (
        db.table("job_runs").delete().lt("started_at", (now - timedelta(days=30)).isoformat()).execute()
    )
    return {"events_deleted": len(old_events.data), "job_runs_deleted": len(old_runs.data)}
