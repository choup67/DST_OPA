-- equivalent distinct symbol mais moins couteux en ressource
select
    e.symbol,
    e.base_asset,
    e.quote_asset
from {{ ref('stg_binance__exchange_info') }} e
where exists (
    select 1
    from {{ ref('stg_binance__kline') }} k
    where k.symbol = e.symbol
)