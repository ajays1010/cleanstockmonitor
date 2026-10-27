from app.core.categories import CATEGORIES


def test_dashboard_shows_empty_watchlist(signed_in):
    resp = signed_in.get("/dashboard")
    assert resp.status_code == 200
    assert "Nothing on your watchlist yet" in resp.text


def test_search_finds_by_symbol_and_name(signed_in):
    resp = signed_in.get("/watchlist/search", params={"q": "tcs"})
    assert "Tata Consultancy Services Ltd" in resp.text
    resp = signed_in.get("/watchlist/search", params={"q": "tata"})
    assert "Tata Power Company Ltd" in resp.text and "Reliance" not in resp.text


def test_search_needs_two_characters(signed_in):
    assert "<li>" not in signed_in.get("/watchlist/search", params={"q": "t"}).text


def test_search_requires_login(client):
    assert client.get("/watchlist/search", params={"q": "tata"}).status_code == 303


def test_add_and_remove(signed_in, watchlist):
    resp = signed_in.post("/watchlist/add", data={"scrip_id": "2"})
    assert resp.status_code == 200
    assert "Tata Power Company Ltd" in resp.text
    assert "Move of 5% or more" in resp.text
    item_id = next(iter(watchlist.rows))

    assert "Watching" in signed_in.get("/watchlist/search", params={"q": "tatapower"}).text

    resp = signed_in.post(f"/watchlist/{item_id}/remove")
    assert "Nothing on your watchlist yet" in resp.text


def test_adding_twice_keeps_one_row(signed_in, watchlist):
    signed_in.post("/watchlist/add", data={"scrip_id": "1"})
    signed_in.post("/watchlist/add", data={"scrip_id": "1"})
    assert len(watchlist.rows) == 1


def test_add_unknown_scrip_shows_error(signed_in):
    resp = signed_in.post("/watchlist/add", data={"scrip_id": "999"})
    assert resp.status_code == 400
    assert "not found" in resp.text


def test_save_settings(signed_in, watchlist):
    signed_in.post("/watchlist/add", data={"scrip_id": "1"})
    item_id = next(iter(watchlist.rows))
    resp = signed_in.post(
        f"/watchlist/{item_id}/settings",
        data={"spike_pct": "3", "price_above": "3000", "price_below": "2,500", "categories": ["results"]},
    )
    assert resp.status_code == 200
    assert "Move of 3% or more" in resp.text
    assert "Above ₹3,000.00" in resp.text and "Below ₹2,500.00" in resp.text
    assert "1 announcement type" in resp.text


def test_invalid_settings_show_error_on_that_scrip(signed_in, watchlist):
    signed_in.post("/watchlist/add", data={"scrip_id": "1"})
    item_id = next(iter(watchlist.rows))
    resp = signed_in.post(
        f"/watchlist/{item_id}/settings", data={"spike_pct": "99", "categories": list(CATEGORIES)}
    )
    assert resp.status_code == 400
    assert "between" in resp.text
    assert "<details open>" in resp.text


def test_cannot_change_another_users_scrip(signed_in, watchlist, fakes):
    signed_in.post("/watchlist/add", data={"scrip_id": "1"})
    item_id = next(iter(watchlist.rows))
    owner, item = watchlist.rows[item_id]
    watchlist.rows[item_id] = ("someone-else", item)

    resp = signed_in.post(
        f"/watchlist/{item_id}/settings", data={"spike_pct": "3", "categories": list(CATEGORIES)}
    )
    assert resp.status_code == 400
    signed_in.post(f"/watchlist/{item_id}/remove")
    assert item_id in watchlist.rows


def test_changes_without_htmx_header_are_refused(signed_in):
    del signed_in.headers["HX-Request"]
    assert signed_in.post("/watchlist/add", data={"scrip_id": "1"}).status_code == 403
