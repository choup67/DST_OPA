USE WAREHOUSE QUERY;
USE DATABASE PAGILA;
USE SCHEMA STAR_SCHEMA;
SELECT *
FROM DIMCUSTOMER
LIMIT 10;


use database DBT;
USE SCHEMA intermediate;
select coingecko_id, asset, count(*) as n
from STG_coingecko__asset_info
where asset is not null
group by 1
having count(*) > 1
order by n desc;

use SCHEMA staging;

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