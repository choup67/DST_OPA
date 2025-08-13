USE WAREHOUSE QUERY;
USE DATABASE PAGILA;
USE SCHEMA STAR_SCHEMA;
SELECT *
FROM DIMCUSTOMER
LIMIT 10;


use database DBT;
USE SCHEMA staging;
USE schema intermediate;
select coingecko_id, asset, count(*) as n
from STG_coingecko__asset_info
where asset is not null
group by 1
having count(*) > 1
order by n desc;

select DISTINCT k.symbol, e.symbol
from int_binance__kline k
left join int_binance__exchange_info e using (symbol);

select DISTINCT e.symbol
from int_binance__exchange_info e
where e.symbol ilike '%4%';

select COUNT(DISTINCT k.symbol)
from int_binance__kline k;

select COUNT(DISTINCT e.symbol)
from int_binance__exchange_info e
where e.symbol ilike '%amd%';

select DISTINCT k.symbol, e.symbol
from int_binance__kline k
left join int_binance__exchange_info e using (symbol);

select DISTINCT k.symbol
from int_binance__kline k
WHERE k.symbol ilike '%USD';

select COUNT(DISTINCT k.symbol), COUNT(DISTINCT e.symbol)
from int_binance__kline k
left join int_binance__exchange_info e using (symbol);

select COUNT(*)
from fact_klines;

select COUNT(DISTINCT k.symbol), COUNT(DISTINCT e.symbol)
from int_binance__kline k
left join int_binance__exchange_info e using (symbol);

select COUNT(DISTINCT symbol)
from int_binance__exchange_info;