-- Utilisation du datawarehouse QUERY
USE WAREHOUSE QUERY;

--Utilisation de la database RAW_DATA et du schéma Coingecko
USE DATABASE RAW_DATA;
USE SCHEMA coingecko;

-- Création de la table assetinfo
CREATE TABLE IF NOT EXISTS asset_info (
    coingecko_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    asset VARCHAR(255) NOT NULL PRIMARY KEY,
    market_cap_rank INT NOT NULL,
    asset_platform VARCHAR(255),
    token_type VARCHAR(255) NOT NULL,
    homepage VARCHAR(),
    categories VARCHAR() NOT NULL,
    supply_circulating DECIMAL(38, 18),
    supply_total DECIMAL(38, 18),
    supply_max DECIMAL(38, 18)
);

--Utilisation de la database RAW_DATA et du schéma Binance
USE DATABASE RAW_DATA;
USE SCHEMA binance;

-- Création de la table exchangeinfo
CREATE TABLE IF NOT EXISTS exchange_info (
    symbol VARCHAR(20) NOT NULL PRIMARY KEY,
    baseAsset VARCHAR(255) NOT NULL,
    quoteAsset VARCHAR(255) NOT NULL,
    status VARCHAR(10) NOT NULL,
    minPrice DECIMAL(20, 8) NOT NULL,
    maxPrice DECIMAL(20, 8) NOT NULL,
    tickSize DECIMAL(20, 8) NOT NULL,
    minQty DECIMAL(20, 8) NOT NULL,
    maxQty DECIMAL(20, 8) NOT NULL,
    stepSize DECIMAL(20, 8) NOT NULL,
    minNotional DECIMAL(20, 8) NOT NULL,
    maxNotional DECIMAL(20, 8) NOT NULL,
    FOREIGN KEY (baseAsset) REFERENCES RAW_DATA.coingecko.asset_info(asset),
    FOREIGN KEY (quoteAsset) REFERENCES RAW_DATA.coingecko.asset_info(asset)
);

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
    taker_buy_base_asset_volume VARCHAR,
    taker_buy_quote_asset_volume VARCHAR,
    ignore VARCHAR,
    symbol VARCHAR(20) NOT NULL FOREIGN KEY REFERENCES exchange_info(symbol)
);