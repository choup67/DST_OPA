/*
-- Utilisation du datawarehouse QUERY
USE WAREHOUSE QUERY;

--Utilisation de la database OPA et du schéma Binance
USE DATABASE RAW_DATA;
USE SCHEMA binance;

-- Création de la table kline
CREATE TABLE IF NOT EXISTS kline (
    id INT AUTOINCREMENT PRIMARY KEY,
    open_time INT NOT NULL,
    open VARCHAR NOT NULL,
    high VARCHAR NOT NULL,
    low VARCHAR NOT NULL,
    close VARCHAR NOT NULL,
    volume VARCHAR NOT NULL,
    close_time INT NOT NULL,
    quote_asset_volume VARCHAR NOT NULL,
    number_of_trades INT NOT NULL,
    taker_buy_base_asset_volume VARCHAR NOT NULL,
    taker_buy_quote_asset_volume VARCHAR NOT NULL,
    symbol VARCHAR(20) NOT NULL
);

-- Création de la table exchange_info
CREATE TABLE IF NOT EXISTS exchange_info (
    symbol VARCHAR(20) NOT NULL PRIMARY KEY,
    base_asset VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    status VARCHAR(10) NOT NULL,
    minPrice DECIMAL(20, 8) NOT NULL,
    maxPrice DECIMAL(20, 8) NOT NULL,
    tickSize DECIMAL(20, 8) NOT NULL,
    minQty DECIMAL(20, 8) NOT NULL,
    maxQty DECIMAL(20, 8) NOT NULL,
    stepSize DECIMAL(20, 8) NOT NULL,
    minNotional DECIMAL(20, 8) NOT NULL,
    maxNotional DECIMAL(20, 8) NOT NULL
);

--Utilisation de la database OPA et du schéma Coingecko
USE DATABASE RAW_DATA;
USE SCHEMA Coingecko;

-- Création de la table asset_info
CREATE TABLE IF NOT EXISTS asset_info (
    coingecko_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    asset VARCHAR(255) NOT NULL PRIMARY KEY,
    market_cap_rank INT NOT NULL,
    asset_plateform VARCHAR(255) NOT NULL,
    token_type VARCHAR(255) NOT NULL,
    homepage VARCHAR(255) NOT NULL,
    categories VARCHAR(255) NOT NULL,
    supply_circulating DECIMAL(38, 18),
    supply_total DECIMAL(38, 18),
    supply_max DECIMAL(38, 18),
    category_type VARCHAR(255) NOT NULL,
);

-- Utilisation de la database OPA et du schéma gdrive
USE DATABASE RAW_DATA;
USE SCHEMA gitrepo;

-- Création de la table fiat_info
CREATE TABLE IF NOT EXISTS fiat_info (
    fiat_name VARCHAR(50) NOT NULL,
    fiat_symbol VARCHAR(10) NOT NULL PRIMARY KEY
);
-- warning, les noms des colonnes présents dans le fichier sont différents

-- Création de la table market_feeling_score
CREATE TABLE IF NOT EXISTS market_feeling_score (
    symbol_type VARCHAR(20) NOT NULL PRIMARY KEY,
    market_feeling_label VARCHAR(50) NOT NULL,
    market_feeling_score VARCHAR(50) NOT NULL
);

-- Création de la table calendar
CREATE TABLE IF NOT EXISTS calendar (
    date_full DATE,
    month INT,
    day INT,
    year INT,
    quarter_id INT,
    month_year_id INT,
    month_year VARCHAR(7),
    weekday_number INT,
    weekday VARCHAR(10),
    quarter VARCHAR(10),
    quarter_year_id INT,
    quarter_year VARCHAR(10)
);

 */