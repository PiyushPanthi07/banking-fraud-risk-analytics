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