with cleaned as (
  select
    upper(symbol)::varchar              as symbol,
    upper(baseAsset)::varchar           as base_asset,
    upper(quoteAsset)::varchar          as quote_asset,
    status::varchar                     as status,
    minPrice::decimal(38,18)            as min_price,
    maxPrice::decimal(38,18)            as max_price,
    tickSize::decimal(38,18)            as tick_size,
    minQty::decimal(38,18)              as min_qty,
    maxQty::decimal(38,18)              as max_qty,
    stepSize::decimal(38,18)            as step_size,
    minNotional::decimal(38,18)         as min_notional,
    maxNotional::decimal(38,18)         as max_notional
  from {{ source('binance','EXCHANGE_INFO') }}
)
select * from cleaned
where status = 'TRADING'  -- on garde uniquement les paires actives