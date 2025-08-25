{{ config(materialized='incremental') }}

with src_lm as (
    select * from {{source('tpch_sample','LINEITEM')}}
)
select * from src_lm