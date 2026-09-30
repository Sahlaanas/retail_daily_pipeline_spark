with source as (
    select * from {{ source('bronze', 'orders') }}
)

select
    order_id,
    customer_id,
    store_id,
    order_date,
    lower(trim(status))    as status,
    total_amount,
    created_at,
    updated_at
from source