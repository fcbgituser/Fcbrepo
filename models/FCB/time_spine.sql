{{ config(materialized='table') }}

with bounds as (
    select
        min(orderdate) as min_date,
        max(orderdate) as max_date
    from {{ ref('fact_sales') }}
),

recursive_dates as (
    select min_date as date_day
    from bounds

    union all

    select dateadd(day, 1, date_day)
    from recursive_dates, bounds
    where date_day < bounds.max_date
)

select date_day
from recursive_dates
