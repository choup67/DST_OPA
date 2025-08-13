select
    symbol,
    base_asset,
    quote_asset
from {{ ref('stg_binance__exchange_info') }}