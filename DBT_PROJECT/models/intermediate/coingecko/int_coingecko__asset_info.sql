with enriched as (
  select
    -- colonnes gardées
    coingecko_id,
    name,
    asset,
    market_cap_rank,
    token_type,
    supply_circulating,
    supply_total,
    supply_max,
    category_type,

    -- flags
    (supply_max is not null) as has_max_supply,
    (supply_total is not null) as has_total_supply,

    -- pourcentages
    case when supply_max > 0 then supply_circulating / supply_max end as pct_of_max_supply,
    case when supply_total > 0 then supply_circulating / supply_total end as pct_of_total_supply,

    -- dispo vs total
    greatest(supply_total - supply_circulating, 0) as locked_supply,
    greatest(supply_total - supply_circulating, 0) / nullif(supply_total, 0) as pct_locked
  from {{ ref('stg_coingecko__asset_info') }}
)

select
  e.*,

  -- stats globales 
  count(distinct asset) over () as total_asset,
  avg(pct_of_max_supply) over () as global_avg_pct_of_max_supply,
  avg(pct_of_total_supply) over () as global_avg_pct_of_total_supply,

  -- stats par catégorie
  count(distinct asset) over (partition by category_type) as cat_total_asset,
  count(distinct asset) over (partition by category_type) / nullif(count(distinct asset) over (), 0) as cat_pct_asset,
  avg(pct_of_max_supply) over (partition by category_type) as cat_avg_pct_of_max_supply,
  avg(pct_of_total_supply) over (partition by category_type) as cat_avg_pct_of_total_supply

from enriched e