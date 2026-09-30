-- dim_date: a standard Kimball date spine. Built natively with
-- Snowflake's GENERATOR table function rather than pulling in
-- dbt-utils just for date_spine — one less dependency for a single
-- straightforward table.

with date_spine as (
    select
        dateadd(day, seq4(), '2023-01-01'::date) as date_day
    from table(generator(rowcount => 1826))  -- 2023-01-01 through 2027-12-31
)

select
    to_number(to_char(date_day, 'YYYYMMDD'))   as date_key,
    date_day,
    year(date_day)                              as year,
    month(date_day)                             as month,
    day(date_day)                               as day,
    quarter(date_day)                           as quarter,
    dayname(date_day)                           as day_name,
    monthname(date_day)                         as month_name,
    dayofweek(date_day) in (0, 6)                as is_weekend
from date_spine