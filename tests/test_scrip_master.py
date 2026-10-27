from app.sources.scrip_master import merge, parse_nse_csv

NSE_MAIN = (
    "SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE\n"
    "ZETWERK,Zetwerk Manufacturing Businesses Limited,EQ,23-OCT-2026,1,1,INE0ZET01011,1\n"
    "BEONLY,Be Series Limited,BE,01-JAN-2020,1,1,INE0BEE01011,1\n"
)
NSE_SME = (
    "SYMBOL,NAME_OF_COMPANY,SERIES,DATE_OF_LISTING,PAID_UP_VALUE,ISIN_NUMBER,FACE_VALUE,\n"
    "SMEONE,Sme One Limited,ST,22-Oct-26,5,INE0SME01011,5,\n"
)


def bse(code, name, isin, symbol, status="Active"):
    return {"SCRIP_CD": code, "Scrip_Name": name, "ISIN_NUMBER": isin, "scrip_id": symbol, "Status": status}


def test_parse_both_nse_csv_layouts():
    nse = parse_nse_csv(NSE_MAIN) | parse_nse_csv(NSE_SME)
    assert nse["INE0ZET01011"].symbol == "ZETWERK" and nse["INE0ZET01011"].series == "EQ"
    assert nse["INE0SME01011"].symbol == "SMEONE" and nse["INE0SME01011"].series == "ST"


def test_merge_maps_symbols_by_isin():
    nse = parse_nse_csv(NSE_MAIN) | parse_nse_csv(NSE_SME)
    rows = merge(
        [
            bse("545000", "Zetwerk Manufacturing Businesses Ltd", "INE0ZET01011", "ZETWERK"),
            bse("540001", "Sme One Ltd", "INE0SME01011", "SMEONE"),
            bse("530001", "Bse Only Ltd", "INE0BSE01011", "BSEONLY"),
            bse("530002", "Be Series Ltd", "INE0BEE01011", "BEONLY"),
        ],
        nse,
    )
    by_code = {r.bse_code: r for r in rows}
    z = by_code["545000"]
    assert (z.nse_symbol, z.yahoo_symbol, z.fyers_symbol) == ("ZETWERK", "ZETWERK.NS", "NSE:ZETWERK-EQ")
    sme = by_code["540001"]
    assert (sme.nse_symbol, sme.fyers_symbol) == ("SMEONE", None)  # Fyers SME symbols come later
    only = by_code["530001"]
    assert (only.nse_symbol, only.yahoo_symbol) == (None, "BSEONLY.BO")
    assert by_code["530002"].fyers_symbol is None  # BE series is not -EQ


def test_merge_skips_inactive_duplicates_and_blanks():
    rows = merge(
        [
            bse("1", "Kept Ltd", None, "KEPT"),
            bse("1", "Duplicate Ltd", None, "DUP"),
            bse("2", "Suspended Ltd", None, "SUSP", status="Suspended"),
            bse("", "No Code Ltd", None, "X"),
        ],
        {},
    )
    assert [(r.bse_code, r.name) for r in rows] == [("1", "Kept Ltd")]
