{{ config(
    tags = ["DIM"]
) }}

with dates as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="dateadd(year,2,CURRENT_TIMESTAMP)"
        )
    }}
)
select
    d.date_day as DATE_DAY,
    dateadd('year',-1, d.DATE_DAY)::date as date_day_pre_year,
    --dateadd('year',-2, d.DATE_DAY)::date as date_day_pre2_year,
    date_trunc('week', d.date_day)::date as week_start_date,
    dateadd('day', 6, date_trunc('week', d.date_day))::date as week_end_date,
    date_part('week', d.date_day)::int as week_number,
    date_trunc('month', d.date_day)::date as month_start_date,
    {{ dbt.last_day('d.date_day', 'month') }} as month_end_date,
    date_part('month', d.date_day)::int as month_number,
    date_trunc('quarter', d.date_day)::date as quarter_start_date,
    {{ dbt.last_day('d.date_day', 'quarter') }} as quarter_end_date,
    date_part('quarter', d.date_day)::int as quarter_number,
    date_trunc('year', d.date_day)::date as year_start_date,
    dateadd('day',-1,dateadd('year', 1, date_trunc('year', d.date_day)))::date as year_end_date,
    date_part('year', d.date_day)::int as year_number,
    date_part('year',dateadd('year',-1, d.DATE_DAY)::date)::int year_number_pre_year,
    --date_part('year',dateadd('year',-2, d.DATE_DAY)::date)::int year_number_pre2_year,
    to_date('2019-'||to_char(current_date,'MM')||'-'||to_char(current_date,'DD'),'YYYY-MM-DD') as DATE_DAY_2019,
    DATEDIFF(DAY,current_date,date_day) as DYN_DAY_NUM,
    (YEAR(week_start_date)||lpad(week(week_start_date),2,0))::int as week_id,
    (YEAR(month_start_date)||lpad(month(month_start_date),2,0))::int as month_id,
    (year_number||0||quarter(quarter_start_date))::int as quarter_id
from
    dates d
order by 1