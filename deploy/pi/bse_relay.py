"""BSE relay for a device on a home connection (a Raspberry Pi). BSE refuses the server,
so this fetches BSE for it: it asks the server what to fetch, makes those requests
unchanged, and posts the raw answers back. The server does everything else.

Needs Python 3 with httpx, and ~/.config/bse-relay.env containing:
    RELAY_URL=https://stockbuzz.duckdns.org
    RELAY_SECRET=<the server's FETCHER_SECRET>

Run every 3 minutes from cron (see deploy/pi/README.md). `--baseline` makes the server
record what is fetched as already sent (used once, when switching back to BSE).
"""

import sys
import time
from pathlib import Path

import httpx

CONFIG = Path.home() / ".config" / "bse-relay.env"


def load_config() -> dict[str, str]:
    pairs = (line.split("=", 1) for line in CONFIG.read_text().splitlines() if "=" in line)
    return {k.strip(): v.strip() for k, v in pairs if not k.strip().startswith("#")}


def fetch(tasks: list[dict], headers: dict, delay: float) -> dict[str, dict]:
    results = {}
    with httpx.Client(headers=headers, timeout=90) as bse:
        for task in tasks:
            try:
                resp = bse.get(task["url"], params=task["params"])
                try:
                    body = resp.json()
                except ValueError:
                    body = resp.text[:300]
                results[task["id"]] = {"status": resp.status_code, "body": body}
            except httpx.HTTPError as exc:
                results[task["id"]] = {"error": f"{type(exc).__name__}: {exc}"}
            time.sleep(delay)
    return results


def main() -> int:
    config = load_config()
    server = httpx.Client(
        base_url=config["RELAY_URL"], headers={"X-Fetcher-Key": config["RELAY_SECRET"]}, timeout=120
    )
    with server:
        plan = server.get("/relay/bse/tasks")
        plan.raise_for_status()
        plan = plan.json()
        if not plan["tasks"]:
            return 0
        results = fetch(plan["tasks"], plan["headers"], plan.get("delay", 0.3))
        failed = [k for k, r in results.items() if r.get("error") or r.get("status") != 200]
        sent = server.post(
            "/relay/bse/results", json={"results": results, "baseline": "--baseline" in sys.argv}
        )
        sent.raise_for_status()
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} fetched {len(results)}, failed {len(failed)} {failed or ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
