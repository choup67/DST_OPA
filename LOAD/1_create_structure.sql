-- Création des utilisateurs et des rôles
-- fait directement dans snowflake UI pour éviter les problèmes de permissions
/* CREATE USER celine
PASSWORD = 'D4taScientest!1234'
DEFAULT_ROLE = SYSADMIN
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE SYSADMIN TO USER celine;
CREATE USER morgane
PASSWORD = 'D4taScientest!'
DEFAULT_ROLE = SYSADMIN
MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE SYSADMIN TO USER morgane; */


-- Création des warehouse
  -- Warehouse pour les requêtes
CREATE WAREHOUSE QUERY 
  WITH WAREHOUSE_SIZE = 'SMALL' 
  INITIALLY_SUSPENDED = TRUE 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE;

  -- Warehouse pour le chargement des données
CREATE WAREHOUSE LOAD 
  WITH WAREHOUSE_SIZE = 'LARGE' 
  INITIALLY_SUSPENDED = TRUE 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE;


-- Utilisation du datawarehouse QUERY
USE WAREHOUSE QUERY;

-- Création de la database RAW_DATA et des schémas nécessaires
CREATE DATABASE IF NOT EXISTS RAW_DATA;
USE DATABASE RAW_DATA;
CREATE SCHEMA IF NOT EXISTS RAW_DATA.stage;
CREATE SCHEMA IF NOT EXISTS RAW_DATA.binance;
CREATE SCHEMA IF NOT EXISTS RAW_DATA.coingecko;
CREATE SCHEMA IF NOT EXISTS RAW_DATA.calendar;

-- Création du fichier file format pour les fichiers CSV
USE SCHEMA RAW_DATA.stage;
CREATE OR REPLACE FILE FORMAT RAW_DATA.STAGE.CLASSIC_CSV
  TYPE = CSV
  COMPRESSION = 'AUTO'                --Compression automatique si applicable (utile si les fichiers sont compressés comme .gz, .bz2, etc.)
  RECORD_DELIMITER = '\n'                 --Chaque ligne dans le fichier représente un enregistrement. Les lignes sont séparées par des sauts de ligne (\n).
  FIELD_DELIMITER = ','                   --Les champs sont séparés par une virgule (typique des fichiers CSV).
  SKIP_HEADER = 1                         --Ignore la première ligne du fichier, souvent utilisée pour les en-têtes de colonnes.
  DATE_FORMAT = 'AUTO'        
  TIMESTAMP_FORMAT = 'AUTO'               --Auto-détection des formats de date et timestamp selon les valeurs du fichier.
  TRIM_SPACE = FALSE                      --Ne pas supprimer les espaces autour des valeurs des champs.
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE   --Génère une erreur si le nombre de colonnes dans un enregistrement ne correspond pas au schéma attendu.
  ESCAPE = 'NONE'                         --Aucun caractère d’échappement utilisé dans les champs entre guillemets.
  ESCAPE_UNENCLOSED_FIELD = '\134'        --Définit \ (code ASCII 134 en octal) comme caractère d’échappement pour les champs non entourés.
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'      --Categories dans coingeck contient des virgules, on peut les entourer de guillemets.
  NULL_IF = ('\\N');                      --Si une valeur est \N, elle sera interprétée comme NULL.


---Création du stage interne pour le chargement des données
USE SCHEMA RAW_DATA.STAGE;

-- création du stage interne
CREATE OR REPLACE STAGE RAW_DATA.STAGE.OPA_STAGE
  FILE_FORMAT = RAW_DATA.STAGE.CLASSIC_CSV
  COMMENT = 'Stage for loading data from CSV files';

LIST @RAW_DATA.STAGE.OPA_STAGE;

-- Création de la database DBT et des schémas nécessaires
CREATE DATABASE IF NOT EXISTS DBT;
USE DATABASE DBT;
CREATE SCHEMA IF NOT EXISTS DBT.staging;
CREATE SCHEMA IF NOT EXISTS DBT.intermediate;
CREATE SCHEMA IF NOT EXISTS DBT.mart;

USE WAREHOUSE QUERY;