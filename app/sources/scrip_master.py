"""Current list of listed equities: BSE's active list, matched by ISIN to NSE symbols.

BSE is the base because announcements are keyed by BSE scrip code. NSE symbols come
from NSE's main-board and SME lists. Companies listed only on NSE are left out for now.
"""

import csv
import io
from dataclasses import dataclass

import httpx

from app.sources import BROWSER_USER_AGENT
from app.sources.bse import HEADERS as BSE_HEADERS

BSE_LIST = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
NSE_MAIN = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_SME = "https://archives.nseindia.com/emerge/corporates/content/SME_EQUITY_L.csv"


class ScripListError(Exception):
    pass


@dataclass(frozen=True)
class NseListing:
    symbol: str
    series: str


@dataclass(frozen=True)
class ScripRow:
    bse_code: str
    bse_symbol: str | None
    name: str
    isin: str | None
    nse_symbol: str | None
    yahoo_symbol: str | None
    fyers_symbol: str | None

    def as_db(self) -> dict:
        return {
            "bse_code": self.bse_code,
            "bse_symbol": self.bse_symbol,
            "name": self.name,
            "isin": self.isin,
            "nse_symbol": self.nse_symbol,
            "yahoo_symbol": self.yahoo_symbol,
            "fyers_symbol": self.fyers_symbol,
            "is_active": True,
        }


def _clean_keys(row: dict) -> dict:
    # NSE's CSV headers carry stray spaces (" ISIN NUMBER") and differ between files.
    return {k.strip().upper().replace("_", " "): (v or "").strip() for k, v in row.items() if k}


def parse_nse_csv(text: str) -> dict[str, NseListing]:
    """ISIN -> NSE listing."""
    out = {}
    for raw in csv.DictReader(io.StringIO(text)):
        row = _clean_keys(raw)
        isin, symbol = row.get("ISIN NUMBER"), row.get("SYMBOL")
        if isin and symbol:
            out[isin] = NseListing(symbol=symbol, series=row.get("SERIES", ""))
    return out


def merge(bse_rows: list[dict], nse: dict[str, NseListing]) -> list[ScripRow]:
    out, seen = [], set()
    for r in bse_rows:
        code = str(r.get("SCRIP_CD") or "").strip()
        name = " ".join((r.get("Scrip_Name") or r.get("Issuer_Name") or "").split())
        if not code or not name or code in seen or (r.get("Status") or "Active") != "Active":
            continue
        seen.add(code)
        isin = (r.get("ISIN_NUMBER") or "").strip() or None
        bse_symbol = (r.get("scrip_id") or "").strip() or None
        listing = nse.get(isin) if isin else None
        nse_symbol = listing.symbol if listing else None
        if nse_symbol:
            yahoo = f"{nse_symbol}.NS"
        elif bse_symbol:
            yahoo = f"{bse_symbol}.BO"  # Yahoo uses BSE's ticker, not the numeric code
        else:
            yahoo = None
        # Fyers symbols for SME and BSE-only scrips come later from the Fyers symbol master.
        fyers = f"NSE:{nse_symbol}-EQ" if listing and listing.series == "EQ" else None
        out.append(ScripRow(code, bse_symbol, name, isin, nse_symbol, yahoo, fyers))
    return out


BSE_LIST_PARAMS = {"Group": "", "Scripcode": "", "industry": "", "segment": "Equity", "status": "Active"}


def fetch_all(client: httpx.Client, bse_rows: list[dict] | None = None) -> list[ScripRow]:
    """All active BSE scrips merged with NSE's lists. bse_rows, when given, is BSE's list
    already fetched elsewhere (the home relay); BSE refuses this server."""
    if bse_rows is None:
        bse_rows = _fetch_bse_list(client)
    if not isinstance(bse_rows, list) or len(bse_rows) < 1000:
        raise ScripListError(f"BSE scrip list looks wrong ({type(bse_rows).__name__}, {len(bse_rows)} rows)")

    nse: dict[str, NseListing] = {}
    for url in (NSE_MAIN, NSE_SME):
        try:
            resp = client.get(url, headers={"User-Agent": BROWSER_USER_AGENT}, timeout=60)
            resp.raise_for_status()
            nse.update(parse_nse_csv(resp.text))
        except httpx.HTTPError as exc:
            raise ScripListError(f"NSE list {url}: {exc}") from exc
    return merge(bse_rows, nse)


def _fetch_bse_list(client: httpx.Client) -> list[dict]:
    try:
        bse = client.get(BSE_LIST, params=BSE_LIST_PARAMS, headers=BSE_HEADERS, timeout=90)
        bse.raise_for_status()
        return bse.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise ScripListError(f"BSE scrip list: {exc}") from exc
