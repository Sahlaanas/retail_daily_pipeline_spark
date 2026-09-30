-- fact_orders: grain is one row per order.
--
-- The important part of this model is the join to dim_customer. It is
-- NOT "join on customer_id and take whatever the customer looks like
-- today" — that would silently misattribute an order placed while a
-- customer lived in Kochi to their current Dubai record. Instead it's a
-- point-in-time join: match the dimension row whose [valid_from,
-- valid_to) window actually contains the order's date.
--
-- Two distinct failure modes both fall back to the Unknown Member
-- (customer_sk = 'unknown'), and it's worth being clear they're
-- different situations even though they resolve the same way here:
--   1. The order's customer_id was quarantined out of BRONZE.customers,
--      or removed as a duplicate, and so never has ANY row in
--      dim_customer at all (~185 customer_ids, per the warnings surfaced
--      in models/staging/schema.yml).
--   2. The customer_id does exist in dim_customer, but this project's
--      snapshot history only starts from when `dbt snapshot` was first
--      run — so an order dated before a customer's earliest captured
--      version falls outside every window that exists for them.
--
-- store_sk uses the same Unknown Member pattern as dim_product, just
-- without the point-in-time complexity (dim_store is Type 1, so a plain
-- join is sufficient — there's no history to match a date against).

with orders as (
    select * from {{ ref('stg_orders') }}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
),

dim_store as (
    select * from {{ ref('dim_store') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

matched as (
    select
        o.order_id,
        o.customer_id,
        o.store_id,
        o.order_date,
        o.status,
        o.total_amount,
        o.created_at,
        o.updated_at,
        dc.customer_sk
    from orders o
    left join dim_customer dc
        on o.customer_id = dc.customer_id
        and o.order_date >= dc.valid_from
        and (o.order_date < dc.valid_to or dc.valid_to is null)
)

select
    matched.order_id,
    coalesce(matched.customer_sk, 'unknown') as customer_sk,
    coalesce(ds.store_id, -1) as store_sk,
    dd.date_key,
    matched.order_date,
    matched.status,
    matched.total_amount,
    matched.created_at,
    matched.updated_at
from matched
left join dim_store ds
    on matched.store_id = ds.store_id
left join dim_date dd
    on to_number(to_char(matched.order_date::date, 'YYYYMMDD')) = dd.date_key