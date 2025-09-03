{{ config(
    tags = ["DIM"]
) }}

with years as(
select 
    distinct
    year_number
    ,DATEDIFF(YEAR,current_date,date_day) as dyn_year_num
    ,year_start_date
    ,year_end_date
    ,year_number_pre_year
    --,year_number_pre2_year
from {{ref('dim_date')}}
)
select
    year_number
    ,dyn_year_num
    ,year_start_date
    ,year_end_date
    ,year_number_pre_year
    --,year_number_pre2_year
from 
    years
    order by year_number