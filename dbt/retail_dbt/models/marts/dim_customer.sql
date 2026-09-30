-- dim_customer: real SCD Type 2. Each customer can have multiple rows
-- here, one per distinct version of their attributes over time —
-- customer_id repeats across versions, customer_sk (the snapshot's own
-- dbt_scd_id) is unique per version and is what fact tables actually
-- join on.

with snapshot as (
    select
        dbt_scd_id     as customer_sk,
        customer_id,
        trim(name)                 as customer_name,
        lower(trim(email))         as email,
        trim(city)                 as city,
        trim(state)                as state,
        signup_date,
        dbt_valid_from              as valid_from,
        dbt_valid_to                as valid_to,
        (dbt_valid_to is null)       as is_current
    from {{ ref('customers_snapshot') }}
),

-- The Unknown Member: a single sentinel row fact tables can fall back to
-- when a customer_id can't be resolved to any real dimension row at all
-- (see fact_orders — this covers the ~185 orders whose customer was
-- quarantined out of BRONZE.customers or removed as a duplicate before
-- ever reaching this dimension).
unknown_member as (
    select
        'unknown'               as customer_sk,
        -1                      as customer_id,
        'Unknown Customer'      as customer_name,
        cast(null as varchar)   as email,
        cast(null as varchar)   as city,
        cast(null as varchar)   as state,
        cast(null as date)      as signup_date,
        '1900-01-01'::timestamp_ntz as valid_from,
        cast(null as timestamp_ntz) as valid_to,
        true                    as is_current
)

select * from snapshot
union all
select * from unknown_member