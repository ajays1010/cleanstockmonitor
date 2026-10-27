# StockMonitor

BSE announcements, price-spike alerts and a daily summary for the stocks you watch, delivered to Telegram within 5 minutes and never twice.

This is the phase-1 rebuild. The previous Flask app is kept in `legacy/` for reference only and will be deleted once phase 1 is live.

## How it fits together

- **worker** (`app/worker.py`): runs every scheduled job. Pollers fetch each source once for all users and store new items in `events`; fan-out creates one `deliveries` row per subscribed chat (unique per event and chat) and only then sends to Telegram.
- **web** (`app/web/main.py`): FastAPI app for login, watchlist and admin. It only reads and writes the database.
- **Supabase**: Postgres and Auth. Only the backend connects, with the service key.

## Local setup

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows; use .venv/bin/activate on Linux
pip install -e ".[dev]"
cp .env.example .env          # fill in the values
uvicorn app.web.main:app --reload --port 8001
python -m app.worker          # in a second terminal
pytest
```

## Database

Migrations live in `supabase/migrations` and run in file order:

1. `0001_schema.sql`: tables, indexes, row-level security, default settings.
2. `0002_seed_scrips.sql`: about 4,600 scrips, generated from `data/indian_stock_tickers.csv` by `python scripts/build_scrip_seed.py`.
3. `0003_auth_trigger.sql`: creates a `profiles` row when a user signs up.

Apply them with the Supabase CLI (`supabase link`, then `supabase db push`), or paste each file into the SQL editor in order.

## Deploy on the VPS

```bash
git clone https://github.com/ajays1010/cleanstockmonitor.git /opt/stockmonitor
cd /opt/stockmonitor
cp .env.example .env && chmod 600 .env   # fill in; APP_ENV=production, STOCKMONITOR_DOMAIN=<your domain>
docker compose -f docker/docker-compose.yml up -d --build
```

Point the domain's DNS A record at the VPS first; Caddy then gets the HTTPS certificate on its own. Automatic deploys from GitHub Actions come later in phase 1.
