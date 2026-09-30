{% snapshot customers_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

-- Snapshotting the BRONZE source directly, not stg_customers, per dbt's
-- own guidance: if the staging model's transformation logic ever
-- changes later, a view-based snapshot would silently rewrite history
-- retroactively. This way, history is only ever recorded from the raw
-- values as they actually were at each point in time.
select
    customer_id,
    name,
    email,
    city,
    state,
    signup_date,
    updated_at
from {{ source('bronze', 'customers') }}

{% endsnapshot %}