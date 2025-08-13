USE DATABASE DBT;
USE SCHEMA STAGING;

select id, symbol, open_time, close_time,
  open_price, high_price, low_price,
  close_price, volume, quote_asset_volume,
  number_of_trades, taker_buy_base_asset_volume,
  taker_buy_quote_asset_volume,

from stg_binance__kline
;

select count(distinct symbol)
from stg_binance__kline;