with k as (
  -- Données enrichies de prix/volume/trades au jour le jour
  select *
  from {{ ref('int_binance__kline') }}
),
s as (
  -- Infos sur le symbol, avec base/quote
  select symbol, base_asset, quote_asset
  from {{ ref('dim_symbol') }}
)

select
  k.symbol, -- clé vers dim_symbol
  s.base_asset,  -- clé vers dim_asset_base
  s.quote_asset, -- clé vers dim_asset_quote
  cast(date_trunc('day', k.open_time) as date) as date_jour, -- clé vers dim_date

  -- Mesures avec coalesce
  coalesce(k.close_price, 0) as close_price,
  coalesce(k.volume, 0) as volume,
  coalesce(k.number_of_trades, 0) as number_of_trades,
  coalesce(k.var_price_1d, 0) as var_price_1d,
  coalesce(k.var_price_7d, 0) as var_price_7d,
  coalesce(k.var_price_30d, 0) as var_price_30d,
  coalesce(k.var_vol_1d, 0) as var_vol_1d,
  coalesce(k.var_vol_7d, 0) as var_vol_7d,
  coalesce(k.var_vol_30d, 0) as var_vol_30d,
  coalesce(k.var_trades_1d, 0) as var_trades_1d,
  coalesce(k.var_trades_7d, 0) as var_trades_7d,
  coalesce(k.var_trades_30d, 0) as var_trades_30d,
  coalesce(k.avg_vol_7d, 0) as avg_vol_7d,
  coalesce(k.avg_vol_30d, 0) as avg_vol_30d,
  coalesce(k.avg_trades_7d, 0) as avg_trades_7d,
  coalesce(k.avg_trades_30d, 0) as avg_trades_30d,
  coalesce(k.vol_ytd_total, 0) as vol_ytd_total,
  coalesce(k.vol_ytd_avg, 0) as vol_ytd_avg,
  coalesce(k.trades_ytd_total, 0) as trades_ytd_total,
  coalesce(k.trades_ytd_avg, 0) as trades_ytd_avg,
  coalesce(k.price_ytd_change, 0) as price_ytd_change,
  coalesce(k.vol_ytd_change, 0) as vol_ytd_change,
  coalesce(k.trades_ytd_change, 0) as trades_ytd_change,

  current_timestamp() as last_updated
  
from k
join s using (symbol)