select
  symbol,
  open_time,
  close_time,
  open_price,
  high_price,
  low_price,
  close_price,
  volume,
  quote_asset_volume,
  number_of_trades,
  taker_buy_base_asset_volume,
  taker_buy_quote_asset_volume,

  -- variations de prix
  (close_price / nullif(lag(close_price, 1)  over (partition by symbol order by open_time), 0) - 1) as var_price_1d,
  (close_price / nullif(lag(close_price, 7)  over (partition by symbol order by open_time), 0) - 1) as var_price_7d,
  (close_price / nullif(lag(close_price, 30) over (partition by symbol order by open_time), 0) - 1) as var_price_30d,

  -- variations de volume
  (volume / nullif(lag(volume, 1)  over (partition by symbol order by open_time), 0) - 1) as var_vol_1d,
  (volume / nullif(lag(volume, 7)  over (partition by symbol order by open_time), 0) - 1) as var_vol_7d,
  (volume / nullif(lag(volume, 30) over (partition by symbol order by open_time), 0) - 1) as var_vol_30d,

  -- variations du nombre de trades
  (number_of_trades / nullif(lag(number_of_trades, 1)  over (partition by symbol order by open_time), 0) - 1) as var_trades_1d,
  (number_of_trades / nullif(lag(number_of_trades, 7)  over (partition by symbol order by open_time), 0) - 1) as var_trades_7d,
  (number_of_trades / nullif(lag(number_of_trades, 30) over (partition by symbol order by open_time), 0) - 1) as var_trades_30d,

  -- moyennes glissantes
  avg(volume)           over (partition by symbol order by open_time rows between 6 preceding and current row)  as avg_vol_7d,
  avg(volume)           over (partition by symbol order by open_time rows between 29 preceding and current row) as avg_vol_30d,
  avg(number_of_trades) over (partition by symbol order by open_time rows between 6 preceding and current row)  as avg_trades_7d,
  avg(number_of_trades) over (partition by symbol order by open_time rows between 29 preceding and current row) as avg_trades_30d,

  -- YTD cumul et moyenne
  sum(volume)           over (partition by symbol, date_trunc('year', open_time) order by open_time rows between unbounded preceding and current row) as vol_ytd_total,
  avg(volume)           over (partition by symbol, date_trunc('year', open_time) order by open_time rows between unbounded preceding and current row) as vol_ytd_avg,
  sum(number_of_trades) over (partition by symbol, date_trunc('year', open_time) order by open_time rows between unbounded preceding and current row) as trades_ytd_total,
  avg(number_of_trades) over (partition by symbol, date_trunc('year', open_time) order by open_time rows between unbounded preceding and current row) as trades_ytd_avg,

  -- YTD change (%) vs 1er jour de l’année
  (close_price / nullif(first_value(close_price) over (partition by symbol, date_trunc('year', open_time) order by open_time rows between unbounded preceding and current row), 0) - 1) as price_ytd_change,
  (volume / nullif(first_value(volume) over (partition by symbol, date_trunc('year', open_time) order by open_time rows between unbounded preceding and current row), 0) - 1) as vol_ytd_change,
  (number_of_trades / nullif(first_value(number_of_trades) over (partition by symbol, date_trunc('year', open_time) order by open_time rows between unbounded preceding and current row), 0) - 1) as trades_ytd_change,

  -- répartition acheteurs / vendeurs (protégée)
  taker_buy_base_asset_volume  / nullif(volume, 0)            as pct_buy_base,
  1 - (taker_buy_base_asset_volume / nullif(volume, 0))       as pct_sell_base,
  taker_buy_quote_asset_volume / nullif(quote_asset_volume, 0) as pct_buy_quote

from {{ ref('stg_binance__kline') }}