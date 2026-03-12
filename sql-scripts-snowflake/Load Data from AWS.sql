-- ================================================================
--  BANKING FRAUD & RISK ANALYTICS
--  Phase 0 + Phase 1 ONLY — Architecture + Bronze Layer
--  S3 Bucket : banking-fraud-risk-analytics
--  S3 Folder : Banking Dataset 10M/
--  AWS Region: eu-north-1 (Europe - Stockholm)
--  Author    : Piyush Sensei
-- ================================================================


-- ================================================================
-- PHASE 0 — ARCHITECTURE SETUP
-- ================================================================

USE ROLE ACCOUNTADMIN;

-- Database
CREATE DATABASE IF NOT EXISTS BANKING_DB
    COMMENT = 'Banking Fraud & Risk Analytics';

-- All 3 schemas upfront (empty containers — industry standard)
CREATE SCHEMA IF NOT EXISTS BANKING_DB.bronze
    COMMENT = 'Layer 1: Raw ingested data — unmodified, everything VARCHAR';

CREATE SCHEMA IF NOT EXISTS BANKING_DB.silver
    COMMENT = 'Layer 2: Cleansed, typed, deduplicated — build next';

CREATE SCHEMA IF NOT EXISTS BANKING_DB.gold
    COMMENT = 'Layer 3: Aggregated, risk-scored, Tableau-ready — build last';

-- Dedicated warehouse (SMALL — sufficient for 10M rows)
CREATE WAREHOUSE IF NOT EXISTS BANKING_WH
    WAREHOUSE_SIZE      = 'SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Warehouse for banking fraud analytics project';

USE WAREHOUSE BANKING_WH;
USE DATABASE  BANKING_DB;
USE SCHEMA    BANKING_DB.bronze;


-- ================================================================
-- PHASE 1 — BRONZE LAYER
-- ================================================================

------------------------------------------------------------------
-- 1.1 Storage Integration
------------------------------------------------------------------
-- Secure IAM trust between Snowflake and your S3 bucket.
-- No AWS keys stored in Snowflake.
CREATE STORAGE INTEGRATION IF NOT EXISTS banking_s3_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::010594764790:role/Snowflake-Data-Pull'
    STORAGE_ALLOWED_LOCATIONS = ('s3://banking-fraud-risk-analytics/banking-dataset/') 
    COMMENT                   = 'S3 integration - banking fraud dataset eu-north-1';

-- PAUSE HERE — run this, then go to AWS IAM and update the trust policy
DESC INTEGRATION banking_s3_integration;
-- Copy these two values into your AWS IAM Role trust policy:
--   STORAGE_AWS_IAM_USER_ARN  -> Principal AWS value
--   STORAGE_AWS_EXTERNAL_ID   -> sts:ExternalId condition value

------------------------------------------------------------------
-- 1.2 File Format (bronze — raw, no type enforcement)
------------------------------------------------------------------
-- No DATE_FORMAT or TIMESTAMP_FORMAT here.
-- Everything lands as raw string. Silver does all the casting.

DROP FILE FORMAT IF EXISTS BANKING_DB.bronze.csv_banking_fmt;

CREATE FILE FORMAT BANKING_DB.bronze.csv_banking_fmt
    TYPE                           = 'CSV'
    FIELD_DELIMITER                = ','
    RECORD_DELIMITER               = '\n'
    PARSE_HEADER                   = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    NULL_IF                        = ('NULL', 'null', 'NA', 'N/A', '')
    EMPTY_FIELD_AS_NULL            = TRUE
    TRIM_SPACE                     = TRUE
    COMPRESSION                    = AUTO
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

------------------------------------------------------------------
-- 1.3 External Stage
------------------------------------------------------------------
-- Pointing at bucket root first so LIST works regardless of
-- folder name spacing. We filter by path in COPY INTO.

CREATE STAGE IF NOT EXISTS BANKING_DB.bronze.banking_raw_stage
    URL                 = 's3://banking-fraud-risk-analytics/banking-dataset/'
    STORAGE_INTEGRATION = banking_s3_integration
    FILE_FORMAT         = BANKING_DB.bronze.csv_banking_fmt
    COMMENT             = 'External stage - S3 banking dataset eu-north-1';

-- List everything in bucket — copy exact file paths from output
LIST @BANKING_DB.bronze.banking_raw_stage;

-- You will see paths like:
--   Banking Dataset 10M/transactions.csv
--   Banking Dataset 10M/customers.csv
--   etc.
-- Use those exact paths in the INFER_SCHEMA and COPY INTO below.

------------------------------------------------------------------ 
-- 1.4 Auto-Create Bronze Tables via INFER_SCHEMA
------------------------------------------------------------------
-- Snowflake reads CSV headers and detects column names.
-- Replace the FILES path with exact path shown in LIST output above.
-- Example: 'transactions.csv'
 
CREATE TABLE IF NOT EXISTS BANKING_DB.bronze.transactions_raw
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION    => '@BANKING_DB.bronze.banking_raw_stage',
                FILE_FORMAT => 'BANKING_DB.bronze.csv_banking_fmt',
                FILES       => 'transactions.csv'
            )
        )
    );
 
CREATE TABLE IF NOT EXISTS BANKING_DB.bronze.customers_raw
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION    => '@BANKING_DB.bronze.banking_raw_stage',
                FILE_FORMAT => 'BANKING_DB.bronze.csv_banking_fmt',
                FILES       => 'customers.csv'
            )
        )
    );
 
CREATE TABLE IF NOT EXISTS BANKING_DB.bronze.accounts_raw
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION    => '@BANKING_DB.bronze.banking_raw_stage',
                FILE_FORMAT => 'BANKING_DB.bronze.csv_banking_fmt',
                FILES       => 'accounts.csv'
            )
        )
    );
 
CREATE TABLE IF NOT EXISTS BANKING_DB.bronze.merchants_raw
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION    => '@BANKING_DB.bronze.banking_raw_stage',
                FILE_FORMAT => 'BANKING_DB.bronze.csv_banking_fmt',
                FILES       => 'merchants.csv'
            )
        )
    );
 
CREATE TABLE IF NOT EXISTS BANKING_DB.bronze.devices_raw
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION    => '@BANKING_DB.bronze.banking_raw_stage',
                FILE_FORMAT => 'BANKING_DB.bronze.csv_banking_fmt',
                FILES       => 'devices.csv'
            )
        )
    );
 
CREATE TABLE IF NOT EXISTS BANKING_DB.bronze.fx_rates_raw
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION    => '@BANKING_DB.bronze.banking_raw_stage',
                FILE_FORMAT => 'BANKING_DB.bronze.csv_banking_fmt',
                FILES       => 'fx_rates.csv'
            )
        )
    );
 
-- Optional: preview what INFER_SCHEMA detected before loading
-- SELECT * FROM TABLE(
--     INFER_SCHEMA(
--         LOCATION    => '@BANKING_DB.bronze.banking_raw_stage',
--         FILE_FORMAT => 'BANKING_DB.bronze.csv_banking_fmt',
--         FILES       => 'transactions.csv'
--     )
-- );
 
------------------------------------------------------------------
-- 1.5 COPY INTO — Load All 6 Files into Bronze
------------------------------------------------------------------
-- MATCH_BY_COLUMN_NAME maps CSV headers to table columns by name.
-- ON_ERROR = CONTINUE skips bad rows without aborting the job.
-- Snowflake tracks loaded files by checksum — re-running is safe,
-- same file will be skipped automatically (no duplicates).
 
COPY INTO BANKING_DB.bronze.transactions_raw
    FROM @BANKING_DB.bronze.banking_raw_stage
    FILES                = ('transactions.csv')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR             = 'CONTINUE';
 
COPY INTO BANKING_DB.bronze.customers_raw
    FROM @BANKING_DB.bronze.banking_raw_stage
    FILES                = ('customers.csv')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR             = 'CONTINUE';
 
COPY INTO BANKING_DB.bronze.accounts_raw
    FROM @BANKING_DB.bronze.banking_raw_stage
    FILES                = ('accounts.csv')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR             = 'CONTINUE';
 
COPY INTO BANKING_DB.bronze.merchants_raw
    FROM @BANKING_DB.bronze.banking_raw_stage
    FILES                = ('merchants.csv')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR             = 'CONTINUE';
 
COPY INTO BANKING_DB.bronze.devices_raw
    FROM @BANKING_DB.bronze.banking_raw_stage
    FILES                = ('devices.csv')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR             = 'CONTINUE';
 
COPY INTO BANKING_DB.bronze.fx_rates_raw
    FROM @BANKING_DB.bronze.banking_raw_stage
    FILES                = ('fx_rates.csv')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR             = 'CONTINUE';

------------------------------------------------------------------ 
-- 1.6 Validate Bronze Load
------------------------------------------------------------------
 
-- Row counts across all 6 tables
SELECT 'transactions_raw' AS table_name, COUNT(*) AS row_count FROM BANKING_DB.bronze.transactions_raw
UNION ALL SELECT 'customers_raw',  COUNT(*) FROM BANKING_DB.bronze.customers_raw
UNION ALL SELECT 'accounts_raw',   COUNT(*) FROM BANKING_DB.bronze.accounts_raw
UNION ALL SELECT 'merchants_raw',  COUNT(*) FROM BANKING_DB.bronze.merchants_raw
UNION ALL SELECT 'devices_raw',    COUNT(*) FROM BANKING_DB.bronze.devices_raw
UNION ALL SELECT 'fx_rates_raw',   COUNT(*) FROM BANKING_DB.bronze.fx_rates_raw
ORDER BY table_name;
 
-- Load history — check for any errors
SELECT
    table_name,
    file_name,
    row_count,
    error_count,
    first_error_message,
    status,
    last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'TRANSACTIONS_RAW',
    START_TIME => DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;
 
-- Quick peek at raw data
SELECT * FROM BANKING_DB.bronze.transactions_raw LIMIT 5;
SELECT * FROM BANKING_DB.bronze.customers_raw    LIMIT 5;
SELECT * FROM BANKING_DB.bronze.accounts_raw     LIMIT 5;
SELECT * FROM BANKING_DB.bronze.merchants_raw    LIMIT 5;
SELECT * FROM BANKING_DB.bronze.devices_raw      LIMIT 5;
SELECT * FROM BANKING_DB.bronze.fx_rates_raw     LIMIT 5;
 
 
-- ================================================================
-- BRONZE COMPLETE
-- Silver layer (casting + cleaning) — built separately next.
-- ================================================================