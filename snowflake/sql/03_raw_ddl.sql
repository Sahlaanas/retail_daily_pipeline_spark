-- 03_raw_ddl.sql
-- One table per source table, matching the raw S3 zone as closely as
-- possible — this layer is meant to be an untouched, replayable copy of
-- what was extracted, before any cleaning happens.

USE DATABASE RETAILPULSE;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW.SUPPLIERS (
    supplier_id     NUMBER,
    supplier_name   VARCHAR,
    country         VARCHAR,
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    year            NUMBER,
    month           NUMBER,
    day             NUMBER
);

CREATE TABLE IF NOT EXISTS RAW.STORES (
    store_id        NUMBER,
    store_name      VARCHAR,
    city            VARCHAR,
    state           VARCHAR,
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    year            NUMBER,
    month           NUMBER,
    day             NUMBER
);

CREATE TABLE IF NOT EXISTS RAW.PRODUCTS (
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

CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS (
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

CREATE TABLE IF NOT EXISTS RAW.ORDERS (
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

CREATE TABLE IF NOT EXISTS RAW.ORDER_ITEMS (
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

CREATE TABLE IF NOT EXISTS RAW.PAYMENTS (
    payment_id      NUMBER,
    order_id        NUMBER,
    payment_method  VARCHAR,
    amount          NUMBER(10, 2),
    payment_status  VARCHAR,
    payment_date    TIMESTAMP_NTZ,
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    year            NUMBER,
    month           NUMBER,
    day             NUMBER
);

CREATE TABLE IF NOT EXISTS RAW.INVENTORY (
    inventory_id        NUMBER,
    product_id          NUMBER,
    store_id            NUMBER,
    quantity_on_hand    NUMBER,
    last_restock_date   DATE,
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    year                NUMBER,
    month               NUMBER,
    day                 NUMBER
);