-- dim_store: Type 1 (current state only), same Unknown Member pattern
-- as dim_product. Reads from stg_stores, which itself reads straight
-- from RAW (not BRONZE) since no bad data was injected into stores in
-- the generator — see the note on the `raw` source in _sources.yml.

with stores as (
    select
        store_id,
        store_name,
        city,
        state
    from {{ ref('stg_stores') }}
),

unknown_member as (
    select
        -1                   as store_id,
        'Unknown Store'      as store_name,
        cast(null as varchar) as city,
        cast(null as varchar) as state
)

select * from stores
union all
select * from unknown_member