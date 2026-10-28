# Deploying to the VPS

The app runs on the shared Ubuntu VPS (172.105.54.28) next to navservice, as three
systemd services under the `stockmonitor` user, behind the existing nginx:

| Service | What it runs |
|---|---|
| `stockmonitor-web` | uvicorn on 127.0.0.1:8002, proxied by nginx at https://stockbuzz.duckdns.org |
| `stockmonitor-worker` | the scheduler: BSE polling, prices, Telegram fan-out, summaries |
| `stockmonitor-bot` | the Telegram bot (long polling; only one copy may run anywhere) |

The Docker files in `docker/` are an alternative; this server has no Docker.

## Updating

```bash
ssh root@172.105.54.28 /opt/stockmonitor/deploy/update.sh
```

Apply new migrations from a dev machine first: `python scripts/apply_migrations.py`.

## Logs

```bash
journalctl -u stockmonitor-worker -f
```

## First install (already done)

```bash
useradd --system --create-home --home-dir /opt/stockmonitor --shell /usr/sbin/nologin stockmonitor
sudo -u stockmonitor git clone -b rebuild/phase1 https://github.com/ajays1010/cleanstockmonitor.git /opt/stockmonitor/src
# (contents moved to /opt/stockmonitor)
sudo -u stockmonitor python3 -m venv /opt/stockmonitor/.venv
sudo -u stockmonitor /opt/stockmonitor/.venv/bin/pip install /opt/stockmonitor
# .env: copy from a dev machine, then set APP_ENV=production and
# APP_BASE_URL=https://stockbuzz.duckdns.org; chmod 600, owned by stockmonitor.
cp deploy/stockmonitor-*.service /etc/systemd/system/ && systemctl daemon-reload
systemctl enable --now stockmonitor-web stockmonitor-worker stockmonitor-bot
cp deploy/nginx-stockbuzz.conf /etc/nginx/sites-available/stockbuzz
ln -s /etc/nginx/sites-available/stockbuzz /etc/nginx/sites-enabled/stockbuzz
nginx -t && systemctl reload nginx
certbot --nginx -d stockbuzz.duckdns.org --redirect
```

Outside the server:
- Supabase → Authentication → URL Configuration: Site URL and redirect URL
  `https://stockbuzz.duckdns.org/auth/callback`.
- Fyers app redirect URL: `https://stockbuzz.duckdns.org/admin/fyers/callback`.
