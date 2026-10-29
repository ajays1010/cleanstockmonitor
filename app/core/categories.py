"""Announcement categories a user can filter on. The announcement poller tags each
BSE filing with one of these keys; an empty watchlist filter means all of them.
Filings tagged ROUTINE (newspaper ads, ESOP grants, call recordings...) are stored but
never sent, whatever the filter."""

CATEGORIES: dict[str, str] = {
    "results": "Results and business updates",
    "orders": "Orders and contracts",
    "investor_meet": "Investor presentations and calls",
    "corporate_action": "Dividends, splits, bonus, buybacks",
    "funding": "Fund raising and acquisitions",
    "management": "Management changes",
    "rating": "Credit ratings",
    "board_meeting": "Board meetings",
    "other": "Other updates",
}

ROUTINE = "routine"
