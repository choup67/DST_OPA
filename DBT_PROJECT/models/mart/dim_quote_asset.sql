{{ config(materialized='view') }} -- on configure en vue pour le rôle quote_asset
with listed as (
  select distinct quote_asset as asset
  from {{ ref('int_binance__kline') }}
)

select
  i.coingecko_id,
  i.name as quote_asset_name,
  l.asset as quote_asset,
  coalesce(i.market_cap_rank, 0) as market_cap_rank,
  i.token_type,
  coalesce(i.supply_circulating, 0) as supply_circulating,
  coalesce(i.supply_total, 0) as supply_total,
  coalesce(i.supply_max, 0) as supply_max,
  i.category_type,
  i.has_max_supply,
  i.has_total_supply,
  coalesce(i.pct_of_max_supply, 0.0)::float as pct_of_max_supply,
  coalesce(i.pct_of_total_supply, 0.0)::float as pct_of_total_supply,
  coalesce(i.locked_supply, 0.0)::float as locked_supply,
  coalesce(i.pct_locked, 0.0)::float as pct_locked,
  current_timestamp() as last_updated
from listed l
join {{ ref('int_coingecko__asset_info') }} i on i.asset = l.asset