-- 04_bronze_ddl.sql
--
-- BRONZE holds the output of the PySpark cleaning job (Part 5): the same
-- columns as RAW, but with bad rows quarantined out and duplicates
-- removed. Only 4 tables so far, because that's what process_raw_data.py
-- currently covers (customers, products, orders, order_items). The
-- remaining 4 (stores, suppliers, payments, inventory) will get their own
-- BRONZE tables once cleaning logic is added for them — until then they
-- can be queried straight from RAW.

USE DATABASE RETAILPULSE;
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS BRONZE.PRODUCTS (
    product_id      NUMBER,
    product_name    VARCHAR,
    category        VARCHAR,
    price           NUMBER(10, 2),
    supplier_id     NUMBER,
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    year            NUMBER,
    month           NUMBER,
    day             NUMBER
);

CREATE TABLE IF NOT EXISTS BRONZE.CUSTOMERS (
    customer_id     NUMBER,
    name            VARCHAR,
    email           VARCHAR,
    city            VARCHAR,
    state           VARCHAR,
    signup_date     DATE,
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    year            NUMBER,
    month           NUMBER,
    day             NUMBER
);

CREATE TABLE IF NOT EXISTS BRONZE.ORDERS (
    order_id        NUMBER,
    customer_id     NUMBER,
    store_id        NUMBER,
    order_date      TIMESTAMP_NTZ,
    status          VARCHAR,
    total_amount    NUMBER(10, 2),
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    year            NUMBER,
    month           NUMBER,
    day             NUMBER
);

CREATE TABLE IF NOT EXISTS BRONZE.ORDER_ITEMS (
    order_item_id   NUMBER,
    order_id        NUMBER,
    product_id      NUMBER,
    quantity        NUMBER,
    unit_price      NUMBER(10, 2),
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    year            NUMBER,
    month           NUMBER,
    day             NUMBER
);