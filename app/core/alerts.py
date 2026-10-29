"""Alert rules and messages, as pure functions over plain dicts so they are easy to test.

Events are stored once in `events`. An event either targets one user (price alerts and
summaries carry payload.user_id) or every watcher of its scrip (announcements). Volume
and DMA alerts are price_spike events too, told apart by payload.rule.
"""

import html
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from app.core.categories import CATEGORIES
from app.core.telegram import esc
from app.core.timeutil import IST
from app.sources.bse import Announcement
from app.sources.quotes import Quote

# An announcement older than this is never sent, so a first poll or a long outage
# does not flood chats with old filings.
MAX_ANNOUNCEMENT_AGE = timedelta(hours=24)

# Telegram's caption limit, counted in UTF-16 units of the text after HTML is parsed.
MAX_CAPTION = 1024
TONE_BADGES = {"positive": "🟢 Positive", "neutral": "⚪ Neutral", "negative": "🔴 Negative"}
# Volume this strong this early in the session is the signal that matters most.
VOLUME_EARLY_BY = time(12, 30)


def announcement_event(a: Announcement, scrip_id: int, nse_symbol: str | None, source: str = "bse") -> dict:
    return {
        "kind": "announcement",
        "scrip_id": scrip_id,
        "source": source,
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


def volume_text(value) -> str:
    """Share counts in Indian units: 85,400 / 1.84 L / 2.10 Cr."""
    n = int(value)
    if n >= 10_000_000:
        return f"{n / 10_000_000:.2f} Cr"
    if n >= 100_000:
        return f"{n / 100_000:.2f} L"
    return f"{n:,}"


def price_events(
    user_id: str, scrip: dict, settings: dict, quote: Quote, today: date, stats: dict | None = None
) -> list[dict]:
    """Price, volume and DMA alerts this quote triggers for one watchlist row. Each fires
    at most once a day per rule (and direction or level), because its source_id carries the
    date. `stats` is the scrip's scrip_stats row (20-day average volume, DMAs), if any."""
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

    stats = stats or {}
    volume = quote.volume
    at = quote.quoted_at.astimezone(IST)
    volume_payload = {**payload, "volume": volume, "at": f"{at:%H:%M}"}
    multiple, average = settings.get("volume_multiple"), stats.get("avg_volume_20")
    if multiple is not None and average and volume is not None:
        ratio = Decimal(volume) / Decimal(average)
        if ratio >= Decimal(str(multiple)):
            out.append(
                {
                    **base,
                    "kind": "price_spike",
                    "source_id": f"volx:{key}:{multiple}",
                    "title": f"{scrip['name']} volume {ratio:.1f}x average",
                    "payload": {
                        **volume_payload,
                        "rule": "volume_x",
                        "avg_volume": average,
                        "ratio": f"{ratio:.1f}",
                        "multiple": str(multiple),
                        "early": at.time() <= VOLUME_EARLY_BY,
                    },
                }
            )
    threshold = settings.get("volume_above")
    if threshold and volume is not None and volume >= int(threshold):
        out.append(
            {
                **base,
                "kind": "price_spike",
                "source_id": f"vola:{key}:{threshold}",
                "title": f"{scrip['name']} volume above {volume_text(threshold)}",
                "payload": {**volume_payload, "rule": "volume_above", "threshold": int(threshold)},
            }
        )
    for period in settings.get("dma_alerts") or []:
        level = stats.get(f"dma_{period}")
        if level is None or quote.prev_close is None:
            continue
        level = Decimal(str(level))
        # A cross is judged against yesterday's close: it fires once, on the day it happens.
        if quote.prev_close < level <= quote.ltp:
            direction = "up"
        elif quote.prev_close > level >= quote.ltp:
            direction = "down"
        else:
            continue
        out.append(
            {
                **base,
                "kind": "price_spike",
                "source_id": f"dma{period}:{key}:{direction}",
                "title": f"{scrip['name']} {'above' if direction == 'up' else 'below'} {period} DMA",
                "payload": {
                    **payload,
                    "rule": "dma",
                    "period": period,
                    "level": str(level),
                    "direction": direction,
                },
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


def _change_pct(ltp, prev_close) -> Decimal | None:
    if ltp is None or not prev_close or Decimal(str(prev_close)) == 0:
        return None
    prev = Decimal(str(prev_close))
    return (Decimal(str(ltp)) - prev) / prev * 100


def change_badge(change: Decimal) -> str:
    """'🟢 +13.33%' when up, '🔴 −2.10%' when down (Telegram has no green arrow emoji)."""
    if change > 0:
        return f"🟢 +{change:.2f}%"
    if change < 0:
        return f"🔴 −{abs(change):.2f}%"
    return "⚪ 0.00%"


def _who(p: dict) -> str:
    tag = p.get("symbol") or f"BSE {p.get('bse_code')}"
    return f"<b>{esc(p.get('company'))}</b> ({esc(tag)})"


def _announcement(event: dict, link: bool, points: int | None = None, title_len: int = 500) -> str:
    p = event.get("payload") or {}
    when = datetime.fromisoformat(event["occurred_at"]).astimezone(IST)
    label = CATEGORIES.get(p.get("category"), "Update")
    bse_label = " · ".join(x for x in (p.get("bse_category"), p.get("bse_subcategory")) if x)
    title = event["title"] if len(event["title"]) <= title_len else event["title"][: title_len - 1] + "…"
    lines = [
        f"📢 {_who(p)}",
        f"<i>{esc(label)}</i>" + (f" · {esc(bse_label)}" if bse_label else ""),
        "",
        esc(title),
    ]
    summary = p.get("summary") or {}
    shown = (summary.get("points") or [])[:points]
    if shown:
        lines += ["", "🤖 <b>AI summary</b> (not advice)", *(f"• {esc(x)}" for x in shown)]
        lines.append(f"Tone: {TONE_BADGES.get(summary.get('tone'), TONE_BADGES['neutral'])}")
    if link and p.get("pdf_url"):
        lines += ["", f'<a href="{esc(p["pdf_url"])}">Open filing (PDF)</a>']
    elif shown:
        lines.append("")
    lines.append(f"🕒 {when:%d %b %Y, %H:%M} IST")
    return "\n".join(lines)


def _visible_len(text: str) -> int:
    plain = html.unescape(re.sub(r"<[^>]+>", "", text))
    return len(plain.encode("utf-16-le")) // 2


def caption(event: dict) -> str:
    """An announcement as the caption of its attached PDF: no link (the file is right
    there), with summary points dropped from the end until it fits Telegram's limit."""
    points = len(((event.get("payload") or {}).get("summary") or {}).get("points") or [])
    for n in range(points, -1, -1):
        text = _announcement(event, link=False, points=n)
        if _visible_len(text) <= MAX_CAPTION:
            return text
    return _announcement(event, link=False, points=0, title_len=300)


def render(event: dict) -> str:
    """Telegram HTML message for an event."""
    p = event.get("payload") or {}
    kind = event["kind"]
    if kind == "announcement":
        return _announcement(event, link=True)
    if kind == "price_spike":
        change = _change_pct(p.get("ltp"), p.get("prev_close"))
        rule = p.get("rule")
        if rule == "spike":
            icon = "📈" if p.get("direction") == "up" else "📉"
            lines = [f"{icon} Price Alert: {_who(p)}"]
        elif rule == "volume_x":
            early = " ⭐ before midday" if p.get("early") else ""
            lines = [
                f"📊 Volume Alert: {_who(p)}",
                f"Volume: {volume_text(p['volume'])} by {p['at']} "
                f"({p['ratio']}× 20-day avg of {volume_text(p['avg_volume'])}){early}",
            ]
        elif rule == "volume_above":
            lines = [
                f"📊 Volume Alert: {_who(p)}",
                f"Volume crossed {volume_text(p['threshold'])}: now {volume_text(p['volume'])} by {p['at']}",
            ]
            if p.get("day_high") and p.get("day_high") == p.get("day_low"):
                lines.append(
                    f"Traded only at {_money(p['ltp'])} today, so it looks circuit-locked; "
                    "rising volume can mean the circuit is about to open."
                )
        elif rule == "dma":
            word = "Crossed above" if p.get("direction") == "up" else "Fell below"
            icon = "📐🟢" if p.get("direction") == "up" else "📐🔴"
            lines = [f"{icon} DMA Alert: {_who(p)}", f"{word} its {p['period']} DMA ({_money(p['level'])})"]
        else:
            word = "above" if rule == "above" else "below"
            lines = [f"🔔 Price Alert: {_who(p)}", f"Crossed {word} {_money(p['level'])}"]
        price = f"Price: {_money(p['ltp'])}"
        if change is not None:
            price += f" ({change_badge(change)}) vs prev close {_money(p['prev_close'])}"
        lines.append(price)
        if p.get("day_high") and p.get("day_low"):
            lines.append(f"Day (High–Low): {_money(p['day_high'])} – {_money(p['day_low'])}")
        return "\n".join(lines)
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
            price = f"{_money(q['ltp'])} ({change_badge(_change_pct(q['ltp'], q['prev_close']))})"
        elif q:
            price = _money(q["ltp"])
        else:
            price = "no price today"
        count = announcement_counts.get(r["id"], 0)
        news = f" · {count} filing{'s' if count != 1 else ''}" if count else ""
        lines.append(f"• {esc(tag)}: {price}{news}")
    return "\n".join(lines)
