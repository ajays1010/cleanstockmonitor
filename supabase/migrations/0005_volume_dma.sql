-- Volume and DMA alerts. All three settings are optional per watchlist row (null or empty = off).
alter table watchlist
  add column volume_multiple numeric(5, 2),  -- alert when today's volume reaches this many times the 20-day average
  add column volume_above bigint,            -- alert when today's volume reaches this many shares
  add column dma_alerts smallint[] not null default '{}';  -- DMAs (20, 50, 200) whose crossing alerts

-- Per-scrip figures from daily history, refreshed twice a day by update_scrip_stats.
create table scrip_stats (
  scrip_id bigint primary key references scrips,
  as_of date not null,  -- last completed session included
  avg_volume_20 bigint,
  dma_20 numeric,
  dma_50 numeric,
  dma_200 numeric,      -- null until a scrip has 200 sessions of history
  updated_at timestamptz not null default now()
);

alter table scrip_stats enable row level security;
