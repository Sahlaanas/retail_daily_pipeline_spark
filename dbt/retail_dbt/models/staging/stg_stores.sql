with source as (
    select * from {{ source('raw', 'stores') }}
)

select
    store_id,
    trim(store_name)    as store_name,
    trim(city)          as city,
    trim(state)         as state,
    created_at,
    updated_at
from source