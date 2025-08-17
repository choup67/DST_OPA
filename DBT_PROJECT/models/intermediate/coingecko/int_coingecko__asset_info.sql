-- pour les tokens présents sur plusieurs blockchain, on prend totken_type native et si ça n'existe pas celui avec le meilleur rank

-- pour les tokens présents sur plusieurs blockchain, on prend token_type native et si ça n'existe pas celui avec le meilleur rank

with ranked as (
  select
    -- colonnes gardées
    coingecko_id,
    name,
    upper(asset) as asset,
    market_cap_rank,
    token_type,
    supply_circulating,
    supply_total,
    supply_max,
    category_type,
    row_number() over (
      partition by upper(asset)
      order by
        case when token_type = 'native' then 1 else 2 end,
        market_cap_rank asc nulls last,
        coingecko_id
    ) as rn
  from {{ ref('stg_coingecko__asset_info') }}
),

enriched as (
  select
    -- colonnes gardées
    coalesce(coingecko_id, '') as coingecko_id,
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
  from ranked
  where rn = 1
)

select
  e.*,

  -- stats globales 
  avg(pct_of_max_supply) over () as global_avg_pct_of_max_supply,
  avg(pct_of_total_supply) over () as global_avg_pct_of_total_supply,

  -- stats par catégorie
  avg(pct_of_max_supply) over (partition by category_type) as cat_avg_pct_of_max_supply,
  avg(pct_of_total_supply) over (partition by category_type) as cat_avg_pct_of_total_supply

from enriched e