-- Utilisation du datawarehouse QUERY
USE WAREHOUSE QUERY;

-- Utilisation de la database RAW_DATA et du schéma calendar
USE DATABASE RAW_DATA;
USE SCHEMA calendar;

CREATE OR REPLACE TABLE calendar AS
WITH dates AS (
  SELECT DATEADD(day, seq4(), DATE '2019-01-01') AS date_full
  FROM TABLE(GENERATOR(ROWCOUNT => 365 * 10))  -- 10 ans
)
SELECT
  date_full,
  -- Parties de date
  EXTRACT(MONTH FROM date_full) AS month,
  EXTRACT(DAY FROM date_full) AS day,
  EXTRACT(YEAR FROM date_full) AS year,
  EXTRACT(QUARTER FROM date_full) AS quarter,
  -- Libellés lisibles
  TO_CHAR(date_full, 'Mon') AS month_name,
  TO_CHAR(date_full, 'DY') AS day_name,
  -- Mois + année
  TO_NUMBER(TO_CHAR(date_full, 'YYYYMM')) AS month_year_id,
  TO_CHAR(date_full, 'YYYY-MM') AS month_year,
  -- Jour de la semaine
  DATE_PART(DAYOFWEEK_ISO, date_full) AS weekday_number,       -- 1 = dimanche                     -- ex: MON, TUE
  CASE
    WHEN DATE_PART(DAYOFWEEK_ISO, date_full) IN (6, 7) THEN FALSE
    ELSE TRUE
  END AS is_weekday,
  -- Quarter
  'Q' || EXTRACT(QUARTER FROM date_full) AS quarter_label,
  -- Quarter + année
  TO_NUMBER(TO_CHAR(date_full, 'YYYY') || EXTRACT(QUARTER FROM date_full)) AS quarter_year_id,
  TO_CHAR(date_full, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM date_full) AS quarter_year
FROM dates;

--- Remplissage de la table FIAT_INFO
-- Utilisation du datawarehouse QUERY
USE WAREHOUSE QUERY;
-- Utilisation de la database RAW_DATA et du schéma calendar
USE DATABASE RAW_DATA;
USE SCHEMA STAGE;

-- Remplissage de la table depuis le fichier dans le stage
COPY INTO RAW_DATA.GITREPO.FIAT_INFO
FROM @OPA_STAGE/table_fiat.csv
FILE_FORMAT = CLASSIC_CSV;

-- Remplissage de la table MARKET_FEELING_SCORE depuis le fichier dans le stage
COPY INTO RAW_DATA.GITREPO.MARKET_FEELING_SCORE
FROM @OPA_STAGE/market_feeling_score.csv
FILE_FORMAT = CLASSIC_CSV;

-- Remplissage de la table asset_info depuis le fichier dans le stage
-- Verification avant chargement
COPY INTO RAW_DATA.coingecko.asset_info
FROM @OPA_STAGE/coingecko_top1000_token_info.csv
FILE_FORMAT = CLASSIC_CSV
VALIDATION_MODE = 'RETURN_ERRORS';
-- chargement
COPY INTO RAW_DATA.coingecko.asset_info
FROM @OPA_STAGE/coingecko_top1000_token_info.csv
FILE_FORMAT = CLASSIC_CSV
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE; 

-- Verification avant chargement
COPY INTO RAW_DATA.binance.exchange_info
FROM @OPA_STAGE/binance_exchange_info.csv
FILE_FORMAT = CLASSIC_CSV
VALIDATION_MODE = 'RETURN_ERRORS';
-- chargement
COPY INTO RAW_DATA.binance.exchange_info
FROM @OPA_STAGE/binance_exchange_info.csv
FILE_FORMAT = CLASSIC_CSV
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Remplissage de la table kline depuis le fichier dans le stage
-- Verification avant chargement
COPY INTO RAW_DATA.binance.kline
FROM @OPA_STAGE/binance_klines.csv
FILE_FORMAT = CLASSIC_CSV
VALIDATION_MODE = 'RETURN_ERRORS';

-- chargement
COPY INTO RAW_DATA.BINANCE.KLINE (
  open_time,
  open,
  high,
  low,
  close,
  volume,
  close_time,
  quote_asset_volume,
  number_of_trades,
  taker_buy_base_asset_volume,
  taker_buy_quote_asset_volume,
  ignore,
  symbol
)
FROM @OPA_STAGE/binance_klines.csv
FILE_FORMAT = CLASSIC_CSV
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;

TRUNCATE TABLE RAW_DATA.COINGECKO.ASSET_INFO;