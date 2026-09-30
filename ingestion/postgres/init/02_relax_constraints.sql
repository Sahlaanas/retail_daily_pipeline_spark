-- Relaxes order_items.product_id so it is no longer FK-enforced.
--
-- This is deliberate: we want to be able to seed "invalid product_id"
-- rows as one of the intentional bad-data cases for the downstream data
-- quality framework to catch. Many real OLTP systems have exactly this
-- kind of integrity gap in practice (constraints added late, disabled for
-- performance, or bypassed by bulk loaders) — this reproduces that
-- realistically rather than pretending referential integrity is perfect
-- at the source.
ALTER TABLE order_items
    DROP CONSTRAINT IF EXISTS order_items_product_id_fkey;