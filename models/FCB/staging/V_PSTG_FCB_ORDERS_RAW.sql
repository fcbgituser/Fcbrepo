{{ config(materialized="view")}}

with src_order as ( select * from {{source('FCB','ORDERS_RAW')}} )
select * from src_order