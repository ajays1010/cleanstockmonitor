-- StockMonitor schema, phase 1.
-- Only the backend connects (service role key). RLS is enabled on every table
-- with no policies, so the anon and authenticated roles can read nothing.

create table profiles (
  id uuid primary key references auth.users on delete cascade,
  email text,
  display_name text,
  is_admin boolean not null default false,
  created_at timestamptz not null default now()
);

create table scrips (
  id bigint generated always as identity primary key,
  bse_code text not null unique,
  nse_symbol text,
  yahoo_symbol text,
  fyers_symbol text,
  name text not null,
  isin text,
  is_active boolean not null default true
);

create index scrips_name_search on scrips using gin (to_tsvector('simple', name));
create index scrips_nse_symbol on scrips (nse_symbol);

create table watchlist (
  id bigint generated always as identity primary key,
  user_id uuid not null references profiles on delete cascade,
  scrip_id bigint not null references scrips,
  spike_pct numeric(5, 2) default 5.0,  -- alert when move vs previous close crosses this
  price_above numeric,
  price_below numeric,
  categories text[] not null default '{}',  -- BSE announcement categories; empty = all
  created_at timestamptz not null default now(),
  unique (user_id, scrip_id)
);

create index watchlist_scrip on watchlist (scrip_id);

create table telegram_chats (
  id bigint generated always as identity primary key,
  user_id uuid not null references profiles on delete cascade,
  chat_id bigint not null,
  label text,  -- shown in the message header
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  unique (user_id, chat_id)
);

create table telegram_link_codes (
  code text primary key,  -- one-time code carried in the bot deep link
  user_id uuid not null references profiles on delete cascade,
  expires_at timestamptz not null
);

create table price_snapshots (
  scrip_id bigint primary key references scrips,
  ltp numeric not null,
  prev_close numeric,
  day_high numeric,
  day_low numeric,
  volume bigint,
  source text not null,  -- 'fyers' or 'yfinance'
  quoted_at timestamptz not null
);

create type event_kind as enum (
  'announcement', 'bulk_deal', 'block_deal', 'price_spike', 'daily_summary'
);

create table events (
  id bigint generated always as identity primary key,
  kind event_kind not null,
  scrip_id bigint references scrips,
  source text not null,     -- 'bse', 'nse', 'fyers', 'system'
  source_id text not null,  -- BSE NEWSID, deal hash, 'spike:<scrip>:<date>:<band>'
  title text not null,
  payload jsonb not null default '{}',  -- category, PDF URL, AI summary, prices
  occurred_at timestamptz not null,
  fanned_out_at timestamptz,  -- set once fan-out has created its deliveries
  created_at timestamptz not null default now(),
  unique (source, source_id)
);

create index events_scrip_time on events (scrip_id, occurred_at desc);
create index events_pending_fan_out on events (created_at) where fanned_out_at is null;

create table deliveries (
  id bigint generated always as identity primary key,
  event_id bigint not null references events on delete cascade,
  chat_id bigint not null,
  user_id uuid not null references profiles on delete cascade,
  status text not null default 'pending' check (status in ('pending', 'sent', 'failed')),
  attempts int not null default 0,
  last_error text,
  created_at timestamptz not null default now(),
  sent_at timestamptz,
  unique (event_id, chat_id)
);

create index deliveries_unsent on deliveries (created_at) where status <> 'sent';

create table job_runs (
  id bigint generated always as identity primary key,
  job text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  ok boolean,
  stats jsonb not null default '{}',
  error text
);

create index job_runs_job_time on job_runs (job, started_at desc);

create table app_settings (
  key text primary key,
  value jsonb not null
);

create table provider_tokens (
  provider text primary key,  -- 'fyers'
  access_token text,
  refresh_token text,
  access_expires_at timestamptz,
  refresh_expires_at timestamptz,
  updated_at timestamptz not null default now()
);

alter table profiles enable row level security;
alter table scrips enable row level security;
alter table watchlist enable row level security;
alter table telegram_chats enable row level security;
alter table telegram_link_codes enable row level security;
alter table price_snapshots enable row level security;
alter table events enable row level security;
alter table deliveries enable row level security;
alter table job_runs enable row level security;
alter table app_settings enable row level security;
alter table provider_tokens enable row level security;

insert into app_settings (key, value) values
  ('daily_summary_time', '"16:00"'),
  ('gemini_daily_pdf_cap', '200'),
  ('market_holidays', '[]');
