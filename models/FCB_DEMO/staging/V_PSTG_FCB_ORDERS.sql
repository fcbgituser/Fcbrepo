{{ config(materialized='incremental') }}

with src_nm as (
    select * from {{source('tpch_sample','NATION')}}
)
select * from src_nm