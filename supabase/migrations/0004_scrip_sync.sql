-- Scrip list refreshed daily from BSE's active equity list plus NSE's main and SME lists.

alter table scrips add column bse_symbol text;
alter table scrips add column updated_at timestamptz not null default now();

create index scrips_bse_symbol on scrips (bse_symbol);
create index scrips_isin on scrips (isin);
