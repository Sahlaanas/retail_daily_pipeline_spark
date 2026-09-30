-- 06_load_bronze.sql
-- Loads the 4 BRONZE tables from s3://retailpulse-data/processed/<table>/
-- — i.e. the cleaned/deduped/validated output of process_raw_data.py,
-- NOT the raw zone.
--
-- TRUNCATE before each COPY INTO on purpose: process_raw_data.py
-- always writes a full current snapshot on every run, under brand-new
-- file names each time — so plain COPY INTO (append-only, and treats
-- any not-previously-seen file name as new data) would silently
-- duplicate every row on every single pipeline run. Truncating first
-- makes each load a full replace, matching how Spark actually
-- produces this data.

USE DATABASE RETAILPULSE;
USE SCHEMA BRONZE;

TRUNCATE TABLE BRONZE.PRODUCTS;

COPY INTO BRONZE.PRODUCTS
    FROM @RETAILPULSE.RAW.RETAILPULSE_S3_STAGE/processed/products/
    FILE_FORMAT = (FORMAT_NAME = RETAILPULSE.RAW.PARQUET_FORMAT)
    PATTERN = '.*[.]parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE BRONZE.CUSTOMERS;

COPY INTO BRONZE.CUSTOMERS
    FROM @RETAILPULSE.RAW.RETAILPULSE_S3_STAGE/processed/customers/
    FILE_FORMAT = (FORMAT_NAME = RETAILPULSE.RAW.PARQUET_FORMAT)
    PATTERN = '.*[.]parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE BRONZE.ORDERS;

COPY INTO BRONZE.ORDERS
    FROM @RETAILPULSE.RAW.RETAILPULSE_S3_STAGE/processed/orders/
    FILE_FORMAT = (FORMAT_NAME = RETAILPULSE.RAW.PARQUET_FORMAT)
    PATTERN = '.*[.]parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE BRONZE.ORDER_ITEMS;

COPY INTO BRONZE.ORDER_ITEMS
    FROM @RETAILPULSE.RAW.RETAILPULSE_S3_STAGE/processed/order_items/
    FILE_FORMAT = (FORMAT_NAME = RETAILPULSE.RAW.PARQUET_FORMAT)
    PATTERN = '.*[.]parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Quick check after running the above — BRONZE counts should be lower
-- than RAW counts (RAW still includes the quarantined/duplicate rows):
-- SELECT 'products' AS table_name, COUNT(*) FROM BRONZE.PRODUCTS
-- UNION ALL SELECT 'customers', COUNT(*) FROM BRONZE.CUSTOMERS
-- UNION ALL SELECT 'orders', COUNT(*) FROM BRONZE.ORDERS
-- UNION ALL SELECT 'order_items', COUNT(*) FROM BRONZE.ORDER_ITEMS;