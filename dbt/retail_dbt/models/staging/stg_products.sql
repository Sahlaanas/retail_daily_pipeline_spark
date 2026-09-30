with source as (
    select * from {{ source('bronze', 'products') }}
)

select
    product_id,
    trim(product_name)     as product_name,
    trim(category)         as category,
    price,
    supplier_id,
    created_at,
    updated_at
from source