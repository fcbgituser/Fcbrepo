{{ config(
    tags = ["DIM"]
) }}

with weeks as(
select 
    distinct
    (year(week_start_date)||lpad(week(week_start_date),2,0))::int as week_id
    ,week_number
    ,'WEEK '||lpad(week(week_start_date),2,0) as week_desc
    ,DATEDIFF(week,current_date,week_start_date) as dyn_week_num
    ,week_start_date
    ,week_end_date
    ,(year(week_start_date)||lpad(month(month_start_date),2,0))::int month_id
    ,(year_number||0||quarter(quarter_start_date))::int as quarter_id
    ,year_number
from {{ref('dim_date')}}
)
select
    week_id
    ,week_number
    ,week_desc
    ,dyn_week_num
    ,week_start_date
    ,week_end_date
    ,month_id
    ,quarter_id
    ,year_number
from 
    weeks
    order by week_start_date