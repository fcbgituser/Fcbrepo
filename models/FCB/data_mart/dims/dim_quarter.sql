{{ config(
    tags = ["DIM"]
) }}

with quarters as(
select 
    distinct
    (year_number||0||quarter(quarter_start_date))::int quarter_id
    ,quarter_number
    ,('Q'||quarter_number||'-'||right(year_number,2)) qtr_desc
    ,DATEDIFF(quarter,current_date,date_day) as dyn_qtr_num
    ,quarter_start_date
    ,quarter_end_date
    ,year_number
from {{ref('dim_date')}}
)
select
    quarter_id
    ,quarter_number
    ,qtr_desc
    ,dyn_qtr_num
    ,quarter_start_date
    ,quarter_end_date
    ,year_number
from 
    quarters
    order by quarter_start_date