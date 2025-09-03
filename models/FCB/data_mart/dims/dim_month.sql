{{ config(
    tags = ["DIM"]
) }}

with months as(
select 
    distinct
    (year(month_start_date)||lpad(MONTH(month_start_date),2,0))::int as month_id
    ,month_number
    ,(MONTHNAME(month_start_date)||'-'||RIGHT(year_number,2) ) as month_desc
    ,DATEDIFF(month,current_date,month_start_date) as dyn_mnth_num
    ,month_start_date
    ,month_end_date
    ,(year_number||0||quarter(quarter_start_date))::int as quarter_id
    ,year_number
from {{ref('dim_date')}}
)
select
    month_id
    ,month_number
    ,month_desc
    ,dyn_mnth_num
    ,month_start_date
    ,month_end_date
    ,quarter_id
    ,year_number
from 
    months
    order by month_start_date