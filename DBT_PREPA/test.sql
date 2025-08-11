USE WAREHOUSE QUERY;
USE DATABASE PAGILA;
USE SCHEMA STAR_SCHEMA;
SELECT *
FROM DIMCUSTOMER
LIMIT 10;


use database DBT;
USE SCHEMA STAGING;
select coingecko_id, asset, count(*) as n
from STG_coingecko__asset_info
where asset is not null
group by 1
having count(*) > 1
order by n desc;

select *
from STG_coingecko__asset_info
where coingecko_id ilike '%binance%';