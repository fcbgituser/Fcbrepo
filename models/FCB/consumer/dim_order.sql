{{ config( materialized="table" )
  
with sat_latest as (
    select
        s.order_pk,
        s.orderstatus,
        s.totalprice,
        s.orderdate,
        s.orderpriority,
        s.clerk,
        s.shippriority,
        s.order_comment,
        s.effective_from,
        row_number() over (
            partition by s.order_pk
            order by s.effective_from desc, s.load_date desc
        ) as rn
    from sat_order_order_details s
)
select 
    -- hash key for the dimension
    md5_binary(order by h.order_pk) as order_sk,
    
    -- Business key from hub
    h.orderkey,
    
    -- Descriptive attributes from satellite
    sl.orderstatus,
    sl.totalprice,
    sl.orderdate,
    sl.orderpriority,
    sl.clerk,
    sl.shippriority,
    sl.order_comment,
    
    -- Optional audit fields
    sl.effective_from,
    h.record_source as hub_record_source,
    sl.record_source as sat_record_source,
    current_timestamp as dim_load_ts
from hub_order h
join sat_latest sl
    on h.order_pk = sl.order_pk
where sl.rn = 1
