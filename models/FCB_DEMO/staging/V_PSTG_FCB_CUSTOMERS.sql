{{ config(materialized='incremental') }}

with src_crm as (
    select * from {{source('tpch_sample','CUSTOMER')}}
)
select * from src_crm