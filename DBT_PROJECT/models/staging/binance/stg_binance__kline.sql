select
  id::int as id,
  upper(symbol)::varchar as symbol,
  to_timestamp_ntz(open_time/1000) as open_time,
  to_timestamp_ntz(close_time/1000) as close_time,
  open::decimal(38,18) as open_price,
  high::decimal(38,18) as high_price,
  low::decimal(38,18) as low_price,
  close::decimal(38,18) as close_price,
  volume::decimal(38,18) as volume,
  quote_asset_volume::decimal(38,18) as quote_asset_volume,
  number_of_trades::int as number_of_trades,
  taker_buy_base_asset_volume::decimal(38,18) as taker_buy_base_asset_volume,
  taker_buy_quote_asset_volume::decimal(38,18) as taker_buy_quote_asset_volume
from {{ source('binance', 'KLINE') }}