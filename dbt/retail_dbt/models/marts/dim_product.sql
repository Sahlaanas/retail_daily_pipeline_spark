-- dim_product: Type 1 (current state only — no history tracking needed
-- here, unlike dim_customer). Includes an Unknown Member row for the
-- same reason as dim_customer: order_items referencing a product_id
-- that was quarantined out of BRONZE.products (negative price) need
-- somewhere valid to point.

with products as (
    select
        product_id,
        product_name,
        category,
        price,
        supplier_id
    from {{ ref('stg_products') }}
),

unknown_member as (
    select
        -1                      as product_id,
        'Unknown Product'       as product_name,
        cast(null as varchar)   as category,
        cast(null as number(10, 2)) as price,
        cast(null as number)    as supplier_id
)

select * from products
union all
select * from unknown_member
