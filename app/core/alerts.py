"""Alert rules and messages, as pure functions over plain dicts so they are easy to test.

Events are stored once in `events`. An event either targets one user (price alerts and
summaries carry payload.user_id) or every watcher of its scrip (announcements).
"""

from datetime import date, datetime, timedelta
from decimal import Decimal

from app.core.categories import CATEGORIES
from app.core.telegram import esc
from app.core.timeutil import IST
from app.sources.bse import Announcement
from app.sources.quotes import Quote

# An announcement older than this is never sent, so a first poll or a long outage
# does not flood chats with old filings.
MAX_ANNOUNCEMENT_AGE = timedelta(hours=24)


def announcement_event(a: Announcement, scrip_id: int, nse_symbol: str | None) -> dict:
    return {
        "kind": "announcement",
        "scrip_id": scrip_id,
        "source": "bse",
        "source_id": a.news_id,
        "title": a.headline[:500],
        "occurred_at": a.published_at.isoformat(),
        "payload": {
            "company": a.company,
            "symbol": nse_symbol,
            "bse_code": a.bse_code,
            "category": a.category,
            "bse_category": a.bse_category,
            "bse_subcategory": a.bse_subcategory,
            "pdf_url": a.pdf_url,
        },
    }


def _money(value: Decimal | float | str) -> str:
    return f"₹{Decimal(str(value)):,.2f}"


def price_events(user_id: str, scrip: dict, settings: dict, quote: Quote, today: date) -> list[dict]:
    """Price alerts this quote triggers for one watchlist row. Each fires at most once a
    day per direction, because its source_id carries the date."""
    out = []
    base = {
        "scrip_id": scrip["id"],
        "source": "prices",
        "occurred_at": quote.quoted_at.isoformat(),
    }
    payload = {
        "user_id": user_id,
        "company": scrip["name"],
        "symbol": scrip.get("nse_symbol"),
        "bse_code": scrip["bse_code"],
        "ltp": str(quote.ltp),
        "prev_close": str(quote.prev_close) if quote.prev_close is not None else None,
        "day_high": str(quote.day_high) if quote.day_high is not None else None,
        "day_low": str(quote.day_low) if quote.day_low is not None else None,
    }
    key = f"{user_id}:{scrip['id']}:{today.isoformat()}"

    change = quote.change_pct
    spike = settings.get("spike_pct")
    if spike is not None and change is not None and abs(change) >= Decimal(str(spike)):
        direction = "up" if change > 0 else "down"
        out.append(
            {
                **base,
                "kind": "price_spike",
                "source_id": f"spike:{key}:{direction}",
                "title": f"{scrip['name']} {direction} {abs(change):.1f}%",
                "payload": {
                    **payload,
                    "rule": "spike",
                    "direction": direction,
                    "change_pct": f"{change:.2f}",
                },
            }
        )
    above = settings.get("price_above")
    if above is not None and quote.ltp >= Decimal(str(above)):
        out.append(
            {
                **base,
                "kind": "price_spike",
                "source_id": f"above:{key}:{above}",
                "title": f"{scrip['name']} above {_money(above)}",
                "payload": {**payload, "rule": "above", "level": str(above)},
            }
        )
    below = settings.get("price_below")
    if below is not None and quote.ltp <= Decimal(str(below)):
        out.append(
            {
                **base,
                "kind": "price_spike",
                "source_id": f"below:{key}:{below}",
                "title": f"{scrip['name']} below {_money(below)}",
                "payload": {**payload, "rule": "below", "level": str(below)},
            }
        )
    return out


def wants_announcement(watch: dict, event: dict, now: datetime) -> bool:
    """Should this watchlist row get this announcement?"""
    occurred = datetime.fromisoformat(event["occurred_at"])
    if now - occurred > MAX_ANNOUNCEMENT_AGE:
        return False
    # Only filings published after the scrip was added (with a little slack for clocks).
    if occurred < datetime.fromisoformat(watch["created_at"]) - timedelta(minutes=5):
        return False
    wanted = watch.get("categories") or []
    return not wanted or event["payload"].get("category") in wanted


def targets(
    event: dict, watchers: list[dict], chats_by_user: dict[str, list[int]], now: datetime
) -> list[tuple[int, str]]:
    """(chat_id, user_id) pairs to deliver to; each chat once even if several users share it."""
    if user_id := event["payload"].get("user_id"):
        users = [user_id]
    else:
        users = [w["user_id"] for w in watchers if wants_announcement(w, event, now)]
    seen, out = set(), []
    for user in users:
        for chat_id in chats_by_user.get(user, []):
            if chat_id not in seen:
                seen.add(chat_id)
                out.append((chat_id, user))
    return out


def _who(p: dict) -> str:
    tag = p.get("symbol") or f"BSE {p.get('bse_code')}"
    return f"<b>{esc(p.get('company'))}</b> ({esc(tag)})"


def render(event: dict) -> str:
    """Telegram HTML message for an event."""
    p = event.get("payload") or {}
    kind = event["kind"]
    if kind == "announcement":
        when = datetime.fromisoformat(event["occurred_at"]).astimezone(IST)
        label = CATEGORIES.get(p.get("category"), "Update")
        bse_label = " · ".join(x for x in (p.get("bse_category"), p.get("bse_subcategory")) if x)
        lines = [
            f"📢 {_who(p)}",
            f"<i>{esc(label)}</i>" + (f" · {esc(bse_label)}" if bse_label else ""),
            "",
            esc(event["title"]),
        ]
        if p.get("pdf_url"):
            lines += ["", f'<a href="{esc(p["pdf_url"])}">Open filing (PDF)</a>']
        lines.append(f"🕒 {when:%d %b %Y, %H:%M} IST")
        return "\n".join(lines)
    if kind == "price_spike":
        detail = []
        if p.get("prev_close"):
            detail.append(f"Prev close {_money(p['prev_close'])}")
        if p.get("day_low") and p.get("day_high"):
            detail.append(f"Day {_money(p['day_low'])} – {_money(p['day_high'])}")
        if p.get("rule") == "spike":
            arrow = "📈" if p.get("direction") == "up" else "📉"
            sign = "+" if p.get("direction") == "up" else ""
            head = f"{arrow} {_who(p)} {sign}{Decimal(p['change_pct']):.2f}% at {_money(p['ltp'])}"
        else:
            word = "above" if p.get("rule") == "above" else "below"
            head = f"🔔 {_who(p)} is {word} {_money(p['level'])}: now {_money(p['ltp'])}"
        return head + ("\n" + " · ".join(detail) if detail else "")
    if kind == "daily_summary":
        return p.get("text") or esc(event["title"])
    return esc(event["title"])


def summary_text(rows: list[dict], announcement_counts: dict[int, int], today: date) -> str:
    """Daily summary for one user. rows: scrip dict + optional 'quote' (price_snapshots row)."""
    lines = [f"📊 <b>Close summary, {today:%d %b %Y}</b>", ""]
    for r in sorted(rows, key=lambda r: r["name"].lower()):
        q = r.get("quote")
        tag = r.get("nse_symbol") or f"BSE {r['bse_code']}"
        if q and q.get("prev_close"):
            ltp, prev = Decimal(str(q["ltp"])), Decimal(str(q["prev_close"]))
            change = (ltp - prev) / prev * 100
            price = f"{_money(ltp)} ({'+' if change >= 0 else ''}{change:.2f}%)"
        elif q:
            price = _money(q["ltp"])
        else:
            price = "no price today"
        count = announcement_counts.get(r["id"], 0)
        news = f" · {count} filing{'s' if count != 1 else ''}" if count else ""
        lines.append(f"• {esc(tag)}: {price}{news}")
    return "\n".join(lines)
