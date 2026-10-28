# BSE relay on a Raspberry Pi

BSE's API refuses cloud servers but answers home connections. A Raspberry Pi at home
(currently a Pi 1 Model B, Raspberry Pi OS Lite) runs `bse_relay.py` every 3 minutes:
it asks the server which BSE requests to make, makes them, and posts the raw answers
back (`app/web/routes/relay.py`). It holds no database keys and needs no admin rights.

Server `.env`: `ANNOUNCEMENT_SOURCE=bse`, `BSE_FETCH=remote` (the worker skips BSE and its
watchdog warns on Telegram if relayed runs stop) and `FETCHER_SECRET`.

## Install (as user `pi`)

```bash
python3 -m venv ~/relay-venv && ~/relay-venv/bin/pip install httpx
mkdir -p ~/bse-relay ~/.config
# copy bse_relay.py to ~/bse-relay/, then create ~/.config/bse-relay.env:
#   RELAY_URL=https://stockbuzz.duckdns.org
#   RELAY_SECRET=<server FETCHER_SECRET>
chmod 600 ~/.config/bse-relay.env
crontab -e   # add:
# */3 * * * * $HOME/relay-venv/bin/python $HOME/bse-relay/bse_relay.py >> $HOME/bse-relay/relay.log 2>&1
```

The server decides when to fetch (announcements 07:00-23:59 IST, the scrip list after
06:30 and 20:30), so the cron line runs around the clock and is mostly a no-op at night.

## Switching back to BSE from NSE

Run the relay once with `--baseline` so filings already sent from NSE are recorded, not
repeated, then set `ANNOUNCEMENT_SOURCE=bse` on the server.
