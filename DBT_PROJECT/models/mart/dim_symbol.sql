with e as (
  select symbol, base_asset, quote_asset
  from {{ ref('int_binance__exchange_info') }}
),
b as (
  select asset as base_asset, category_type as base_category, (asset = 'BTC') as base_is_btc
  from {{ ref('dim_base_asset') }}
),
q as (
  select asset as quote_asset, category_type as quote_category, (asset = 'BTC') as quote_is_btc
  from {{ ref('dim_quote_asset') }}
)

select
  e.symbol,
  e.base_asset,
  e.quote_asset,

  -- catégories issues de dim_asset
  b.base_category,
  q.quote_category,

  -- typologie de paire
  case
    when b.base_category is null or q.quote_category is null then null
    else concat(b.base_category, '_', q.quote_category)
  end as symbol_type,

  -- flags utiles pour les filtres
  coalesce(b.base_is_btc or q.quote_is_btc, false) as is_btc_pair,
  coalesce(b.base_category = 'stablecoin' or q.quote_category = 'stablecoin', false) as is_stable_pair,
  coalesce(b.base_category <> 'stablecoin' and q.quote_category <> 'stablecoin', false) as is_cross_crypto,

  current_timestamp() as last_updated

from e
left join b on e.base_asset = b.base_asset
left join q on e.quote_asset = q.quote_asset