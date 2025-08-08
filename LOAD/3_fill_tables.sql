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
  EXTRACT(QUARTER FROM date_full) AS quarter_num,
  -- Libellés lisibles
  TO_CHAR(MONTHNAME(date_full)) AS month_name,
  TO_CHAR(DAYNAME(date_full)) AS day_name,
  -- Mois + année
  TO_NUMBER(TO_CHAR(date_full, 'YYYYMM')) AS month_year_id,
  TO_CHAR(date_full, 'YYYY-MM') AS month_year,
  -- Jour de la semaine (1=lundi ... 7=dimanche)
  DATE_PART(DAYOFWEEK_ISO, date_full) AS weekday_number,
  -- Jour ouvré ou non
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