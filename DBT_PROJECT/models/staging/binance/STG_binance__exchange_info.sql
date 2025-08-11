select *
from {{ source('binance', 'EXCHANGE_INFO') }}
limit 10