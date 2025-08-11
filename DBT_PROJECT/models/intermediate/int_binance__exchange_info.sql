select *
from {{ ref('stg_binance__exchange_info') }}