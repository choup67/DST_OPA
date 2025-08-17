{{ config(materialized='view') }} -- on configure en vue pour le rôle quote_asset
select
  coingecko_id,
  name as quote_asset_name,
  asset as quote_asset,
  coalesce(market_cap_rank, 0) as market_cap_rank,
  token_type,
  coalesce(supply_circulating, 0) as supply_circulating,
  coalesce(supply_total, 0) as supply_total,
  coalesce(supply_max, 0) as supply_max,
  category_type,
  has_max_supply,
  has_total_supply,

  coalesce(pct_of_max_supply, 0) as pct_of_max_supply,
  coalesce(pct_of_total_supply, 0) as pct_of_total_supply,
  coalesce(locked_supply, 0) as locked_supply,
  coalesce(pct_locked, 0) as pct_locked,

  coalesce(global_avg_pct_of_max_supply, 0) as global_avg_pct_of_max_supply,
  coalesce(global_avg_pct_of_total_supply, 0) as global_avg_pct_of_total_supply,
  coalesce(cat_avg_pct_of_max_supply, 0) as cat_avg_pct_of_max_supply,
  coalesce(cat_avg_pct_of_total_supply, 0) as cat_avg_pct_of_total_supply,

  -- nouvelles agg distinct
  coalesce(total_asset, 0) as total_asset,
  coalesce(cat_total_asset, 0) as cat_total_asset,
  coalesce(cat_pct_asset, 0) as cat_pct_asset,

  current_timestamp() as last_updated

from {{ ref('int_coingecko__asset_info') }}