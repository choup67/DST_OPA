USE WAREHOUSE QUERY;
USE DATABASE PAGILA;
USE SCHEMA STAR_SCHEMA;
SELECT *
FROM DIMCUSTOMER
LIMIT 10;


use database DBT;
USE SCHEMA intermediate;
select coingecko_id, asset, count(*) as n
from stg_coingecko__asset_info
where asset is not null
group by 1
having count(*) > 1
order by n desc;

use SCHEMA staging;
select
  (select count(distinct symbol) from RAW_DATA.BINANCE.EXCHANGE_INFO) as nb_exchange_info,
  (select count(distinct symbol) from RAW_DATA.BINANCE.KLINE) as nb_kline,
  (select count(distinct e.symbol)
     from RAW_DATA.BINANCE.EXCHANGE_INFO e
     join RAW_DATA.BINANCE.KLINE k on e.symbol = k.symbol) as nb_communs;
     
select
  count(distinct e.symbol) as nb_communs,
  count(distinct case when c.asset is not null then e.symbol end) as nb_communs_avec_infos,
  count(distinct case when c.asset is null then e.symbol end) as nb_communs_sans_infos
from RAW_DATA.BINANCE.EXCHANGE_INFO e
join RAW_DATA.BINANCE.KLINE k
  on e.symbol = k.symbol
left join RAW_DATA.COINGECKO.ASSET_INFO c
  on upper(c.asset) = upper(e.baseasset)   -- ⚠️ ou autre logique de mapping
   or upper(c.asset) = upper(e.quoteasset);

select *
from stg_binance__exchange_info;
select *
from int_coingecko__asset_info;

select *
from stg_gitrepo__fiat_info;

select *
from int_binance__kline;

select distinct date_jour, date_full
from fact_klines
left join dim_calendar
on fact_klines.date_jour = dim_calendar.date_full
where dim_calendar.year is  null
order by date_jour asc;

USE SCHEMA mart;

select base_asset, count(*) as nb
from DBT.MART.DIM_BASE_ASSET
group by base_asset
having count(*) > 1
order by nb desc;

select asset, coingecko_id, name, token_type, category_type, market_cap_rank
from DBT.INTERMEDIATE.INT_COINGECKO__ASSET_INFO
qualify count(*) over (partition by asset) > 1
order by asset, market_cap_rank nulls last;

select *
from dim_quote_asset;

select distinct date_full
from dim_calendar
where date_full = '2019-09-25';

USE DATABASE RAW_DATA;
USE SCHEMA stage;

USE WAREHOUSE QUERY;

select *
from RAW_DATA.coingecko.asset_info;