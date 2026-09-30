-- fact_order_items: grain is one row per order line item.
--
-- Two different fixes for two genuinely different situations, worth
-- not conflating even though both ultimately came from the same
-- staging-layer warnings:
--
--   - product_id: same Unknown Member pattern as everywhere else. A
--     line item pointing at a product that got quarantined for a bad
--     price is still real revenue that happened — it just can't be
--     attributed to a known product record, so it maps to
--     dim_product's Unknown Product row (product_id = -1).
--
--   - order_id: NOT given the same treatment. A line item with no
--     parent order has no order date, no customer, nothing to roll up
--     to at all — there's no sensible "Unknown Order" to invent the way
--     there's a sensible "Unknown Customer". These rows are genuinely
--     excluded here (inner join to fact_orders), the same way Spark's
--     process_raw_data.py already quarantines rows outright rather than
--     inventing a placeholder for something structurally meaningless.
--     This should drop exactly the ~358 rows already identified in the
--     staging-layer warning — if that count ever drifts, the dedup
--     logic upstream changed and this comment (and the reasoning behind
--     it) needs re-checking.

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

fact_orders as (
    select * from {{ ref('fact_orders') }}
),

dim_product as (
    select * from {{ ref('dim_product') }}
),

-- Inner join deliberately: this is where the orphaned order_id rows
-- get dropped, inheriting customer_sk/date_key from fact_orders at the
-- same time so this table doesn't have to re-derive them.
attributed as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        oi.created_at,
        oi.updated_at,
        fo.customer_sk,
        fo.store_sk,
        fo.date_key
    from order_items oi
    inner join fact_orders fo on oi.order_id = fo.order_id
)

select
    attributed.order_item_id,
    attributed.order_id,
    attributed.customer_sk,
    attributed.store_sk,
    attributed.date_key,
    coalesce(dp.product_id, -1) as product_sk,
    attributed.quantity,
    attributed.unit_price,
    attributed.line_total,
    attributed.created_at,
    attributed.updated_at
from attributed
left join dim_product dp
    on attributed.product_id = dp.product_id