"""Announcement categories a user can filter on. The announcement poller tags each
BSE filing with one of these keys; an empty watchlist filter means all of them."""

CATEGORIES: dict[str, str] = {
    "results": "Financial results",
    "board_meeting": "Board meetings",
    "orders": "Orders and contracts",
    "corporate_action": "Dividends, splits, bonus, buybacks",
    "rating": "Credit ratings",
    "investor_meet": "Investor presentations and calls",
    "other": "Other updates",
}
