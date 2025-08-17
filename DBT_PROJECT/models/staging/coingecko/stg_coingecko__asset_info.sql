with cleaned as (
  select
    coingecko_id::varchar             as coingecko_id,
    name::varchar                     as name,
    upper(asset)::varchar             as asset,
    market_cap_rank::int              as market_cap_rank,
    asset_platform::varchar           as asset_platform,
    token_type::varchar               as token_type,
    homepage::varchar                 as homepage,
    categories::varchar               as categories,
    supply_circulating::decimal(38,9) as supply_circulating,
    supply_total::decimal(38,9)       as supply_total,
    supply_max::decimal(38,9)         as supply_max,
    case
      when categories ilike '%stablecoin%' then 'stablecoin'
      when categories ilike '%btc%' or categories ilike '%bitcoin%' then 'bitcoin'
      else 'altcoin'
    end                                as category_type
  from  {{ source('coingecko','ASSET_INFO') }}
)
select * from cleaned