-- Utilisation du datawarehouse QUERY
USE WAREHOUSE QUERY;

-- Utilisation de la database RAW_DATA et du schéma calendar
USE DATABASE RAW_DATA;
USE SCHEMA calendar;

CREATE OR REPLACE TABLE calendar AS
WITH dates AS (
  SELECT DATEADD(day, seq4(), '2020-01-01') AS date_full
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
  TO_CHAR(date_full, 'Month') AS month_name,
  TO_CHAR(date_full, 'Day') AS day_name,
  -- Mois + année
  TO_NUMBER(TO_CHAR(date_full, 'YYYYMM')) AS month_year_id,
  TO_CHAR(date_full, 'YYYY-MM') AS month_year,
  -- Jour de la semaine
  DATE_PART(DAYOFWEEK, date_full) AS weekday_number,       -- 1 = dimanche
  TO_CHAR(date_full, 'DY') AS weekday,                      -- ex: MON, TUE
  CASE
    WHEN DATE_PART(DAYOFWEEK, date_full) IN (1, 7) THEN FALSE
    ELSE TRUE
  END AS is_weekday,
  -- Quarter
  TO_CHAR(date_full, '"Q"Q') AS quarter_label,
  -- Quarter + année
  TO_NUMBER(TO_CHAR(date_full, 'YYYY') || EXTRACT(QUARTER FROM date_full)) AS quarter_year_id,
  TO_CHAR(date_full, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM date_full) AS quarter_year
FROM dates

--- Remplissage de la table FIAT_INFO
-- Utilisation du datawarehouse QUERY
USE WAREHOUSE QUERY;
-- Utilisation de la database RAW_DATA et du schéma calendar
USE DATABASE RAW_DATA;
USE SCHEMA GITREPO;

-- Remplissage de la table depuis le fichier dans le stage
COPY INTO FIAT_INFO
FROM @MY_STAGE/table_fiat.csv
FILE_FORMAT = CLASSIC_CSV;

-- Remplissage de la table MARKET_FEELING_SCORE depuis le fichier dans le stage
COPY INTO MARKET_FEELING_SCORE
FROM @MY_STAGE/market_feeling_score.csv
FILE_FORMAT = CLASSIC_CSV;