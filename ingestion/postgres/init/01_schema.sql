-- RetailPulse OLTP schema
-- Runs once automatically when the postgres container's data volume is empty.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Shared trigger: keeps updated_at current on every row update ───────────
-- This is what makes "updated_at > last_successful_timestamp" incremental
-- extraction actually correct later — without it, updated_at would only
-- ever reflect INSERT time, not later UPDATEs.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── Reference tables ─────────────────────────────────────────────────────
CREATE TABLE suppliers (
    supplier_id     SERIAL PRIMARY KEY,
    supplier_name   TEXT NOT NULL,
    country         TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stores (
    store_id        SERIAL PRIMARY KEY,
    store_name      TEXT NOT NULL,
    city            TEXT,
    state           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    name            TEXT,
    email           TEXT,
    city            TEXT,
    state           TEXT,
    signup_date     DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id      SERIAL PRIMARY KEY,
    product_name    TEXT NOT NULL,
    category        TEXT,
    price           NUMERIC(10, 2),
    supplier_id     INTEGER REFERENCES suppliers(supplier_id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Transactional tables ─────────────────────────────────────────────────
CREATE TABLE orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(customer_id),
    store_id        INTEGER REFERENCES stores(store_id),
    order_date      TIMESTAMPTZ,
    status          TEXT,
    total_amount    NUMERIC(10, 2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER REFERENCES orders(order_id),
    product_id      INTEGER REFERENCES products(product_id),
    quantity        INTEGER,
    unit_price      NUMERIC(10, 2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INTEGER REFERENCES orders(order_id),
    payment_method  TEXT,
    amount          NUMERIC(10, 2),
    payment_status  TEXT,
    payment_date    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory (
    inventory_id        SERIAL PRIMARY KEY,
    product_id          INTEGER REFERENCES products(product_id),
    store_id            INTEGER REFERENCES stores(store_id),
    quantity_on_hand    INTEGER,
    last_restock_date   DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Attach the updated_at trigger to every table above
DO $$
DECLARE
    t TEXT;
BEGIN
    FOR t IN
        SELECT unnest(ARRAY['suppliers', 'stores', 'customers', 'products',
                             'orders', 'order_items', 'payments', 'inventory'])
    LOOP
        EXECUTE format(
            'CREATE TRIGGER trg_set_updated_at BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION set_updated_at();', t);
    END LOOP;
END;
$$;

-- Indexes to make "WHERE updated_at > $watermark" cheap once tables are large
CREATE INDEX idx_customers_updated_at ON customers(updated_at);
CREATE INDEX idx_products_updated_at ON products(updated_at);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_order_items_updated_at ON order_items(updated_at);
CREATE INDEX idx_payments_updated_at ON payments(updated_at);
CREATE INDEX idx_inventory_updated_at ON inventory(updated_at);

-- ── Pipeline watermark table ─────────────────────────────────────────────
-- Tracks, per pipeline, how far incremental extraction has progressed.
CREATE TABLE pipeline_watermark (
    pipeline_name           TEXT PRIMARY KEY,
    last_processed_timestamp TIMESTAMPTZ,
    last_successful_run     TIMESTAMPTZ
);