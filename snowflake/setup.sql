-- ============================================================
-- Streaming Content Analytics — Snowflake Setup
-- Run once before first ingestion
-- ============================================================

-- Warehouse (reuse from Project 1 or create new)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;

-- Database
CREATE DATABASE IF NOT EXISTS STREAMING_DB
    COMMENT = 'Streaming Content Analytics (TMDb movies)';

USE DATABASE STREAMING_DB;

-- Schemas
CREATE SCHEMA IF NOT EXISTS RAW          COMMENT = 'Raw API data';
CREATE SCHEMA IF NOT EXISTS STAGING      COMMENT = 'Cleaned, typed dbt views';
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE COMMENT = 'Enriched intermediate models';
CREATE SCHEMA IF NOT EXISTS MARTS        COMMENT = 'Star schema for Tableau';

-- ============================================================
-- Quick data quality check after ingestion
-- ============================================================

-- Row counts
SELECT table_schema, table_name, row_count
FROM   information_schema.tables
WHERE  table_schema IN ('RAW','STAGING','INTERMEDIATE','MARTS')
ORDER  BY table_schema, table_name;

-- Genre distribution
SELECT primary_genre, COUNT(*) AS cnt
FROM   RAW.TMDB_MOVIES
GROUP  BY 1
ORDER  BY 2 DESC
LIMIT  20;

-- Language distribution
SELECT original_language, COUNT(*) AS cnt
FROM   RAW.TMDB_MOVIES
GROUP  BY 1
ORDER  BY 2 DESC
LIMIT  10;
