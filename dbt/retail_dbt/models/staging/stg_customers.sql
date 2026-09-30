with source as (
    select * from {{ source('bronze', 'customers') }}
)

select
    customer_id,
    trim(name)              as customer_name,
    lower(trim(email))      as email,
    trim(city)              as city,
    trim(state)             as state,
    signup_date,
    created_at,
    updated_at
from source