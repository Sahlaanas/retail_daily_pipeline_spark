-- A singular dbt test: this query should return ZERO rows to pass.
-- Any row returned here is a real data quality violation — the
-- BRONZE/Spark layer is supposed to prevent negative amounts from ever
-- reaching this point, so a failure here means something upstream
-- regressed, not that this test is too strict.

select
    order_id,
    total_amount
from {{ ref('stg_orders') }}
where total_amount < 0