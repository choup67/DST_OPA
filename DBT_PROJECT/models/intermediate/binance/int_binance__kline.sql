select
  k.symbol,
  e.base_asset,
  e.quote_asset,
  k.open_time,
  k.close_time,
  k.open_price,
  k.high_price,
  k.low_price,
  k.close_price,
  k.volume,
  k.quote_asset_volume,
  k.number_of_trades,
  k.taker_buy_base_asset_volume,
  k.taker_buy_quote_asset_volume,

  -- variations de prix
  (k.close_price / nullif(lag(k.close_price, 1)  over (partition by k.symbol order by k.open_time), 0) - 1) as var_price_1d,
  (k.close_price / nullif(lag(k.close_price, 7)  over (partition by k.symbol order by k.open_time), 0) - 1) as var_price_7d,
  (k.close_price / nullif(lag(k.close_price, 30) over (partition by k.symbol order by k.open_time), 0) - 1) as var_price_30d,

  -- variations de volume
  (k.volume / nullif(lag(k.volume, 1)  over (partition by k.symbol order by k.open_time), 0) - 1) as var_vol_1d,
  (k.volume / nullif(lag(k.volume, 7)  over (partition by k.symbol order by k.open_time), 0) - 1) as var_vol_7d,
  (k.volume / nullif(lag(k.volume, 30) over (partition by k.symbol order by k.open_time), 0) - 1) as var_vol_30d,

  -- variations du nombre de trades
  (k.number_of_trades / nullif(lag(k.number_of_trades, 1)  over (partition by k.symbol order by k.open_time), 0) - 1) as var_trades_1d,
  (k.number_of_trades / nullif(lag(k.number_of_trades, 7)  over (partition by k.symbol order by k.open_time), 0) - 1) as var_trades_7d,
  (k.number_of_trades / nullif(lag(k.number_of_trades, 30) over (partition by k.symbol order by k.open_time), 0) - 1) as var_trades_30d,

  -- moyennes glissantes
  avg(k.volume)           over (partition by k.symbol order by k.open_time rows between 6 preceding and current row)  as avg_vol_7d,
  avg(k.volume)           over (partition by k.symbol order by k.open_time rows between 29 preceding and current row) as avg_vol_30d,
  avg(k.number_of_trades) over (partition by k.symbol order by k.open_time rows between 6 preceding and current row)  as avg_trades_7d,
  avg(k.number_of_trades) over (partition by k.symbol order by k.open_time rows between 29 preceding and current row) as avg_trades_30d,

  -- YTD cumul et moyenne
  sum(k.volume)           over (partition by k.symbol, date_trunc('year', k.open_time) order by k.open_time rows between unbounded preceding and current row) as vol_ytd_total,
  avg(k.volume)           over (partition by k.symbol, date_trunc('year', k.open_time) order by k.open_time rows between unbounded preceding and current row) as vol_ytd_avg,
  sum(k.number_of_trades) over (partition by k.symbol, date_trunc('year', k.open_time) order by k.open_time rows between unbounded preceding and current row) as trades_ytd_total,
  avg(k.number_of_trades) over (partition by k.symbol, date_trunc('year', k.open_time) order by k.open_time rows between unbounded preceding and current row) as trades_ytd_avg,

  -- YTD change (%) vs 1er jour de l'année
  (k.close_price / nullif(first_value(k.close_price) over (partition by k.symbol, date_trunc('year', k.open_time) order by k.open_time rows between unbounded preceding and current row), 0) - 1) as price_ytd_change,
  (k.volume / nullif(first_value(k.volume) over (partition by k.symbol, date_trunc('year', k.open_time) order by k.open_time rows between unbounded preceding and current row), 0) - 1) as vol_ytd_change,
  (k.number_of_trades / nullif(first_value(k.number_of_trades) over (partition by k.symbol, date_trunc('year', k.open_time) order by k.open_time rows between unbounded preceding and current row), 0) - 1) as trades_ytd_change,

  -- répartition acheteurs / vendeurs (protégée)
  k.taker_buy_base_asset_volume  / nullif(k.volume, 0)            as pct_buy_base,
  1 - (k.taker_buy_base_asset_volume / nullif(k.volume, 0))       as pct_sell_base,
  k.taker_buy_quote_asset_volume / nullif(k.quote_asset_volume, 0) as pct_buy_quote

from {{ ref('stg_binance__kline') }} k
join {{ ref('stg_binance__exchange_info') }} e on k.symbol = e.symbol