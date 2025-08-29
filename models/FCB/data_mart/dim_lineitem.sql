{{ config( materialized="table" )}}


with sat_latest as (
    select
        s.lineitem_pk,
        s.commitdate,
        s.discount,
        s.extendedprice,
        s.line_comment,
        s.quantity,
        s.receiptdate,
        s.returnflag,
        s.shipdate,
        s.shipinstruct,
        s.shipmode,
        s.tax,
        s.effective_from,
        row_number() over (
            partition by s.lineitem_pk
            order by s.effective_from desc, s.load_date desc
        ) as rn
    from sat_order_lineitem_details s
)
select 
    -- hash key for the dimension
    md5_binary(h.lineitem_pk) as lineitem_hk,
    
    -- Business keys from Hub
    h.linenumber,
    h.orderkey,
    
    -- Attributes from Satellite (latest record only)
    sl.commitdate,
    sl.discount,
    sl.extendedprice,
    sl.line_comment,
    sl.quantity,
    sl.receiptdate,
    sl.returnflag,
    sl.shipdate,
    sl.shipinstruct,
    sl.shipmode,
    sl.tax,
    
    -- Optional audit fields
    sl.effective_from,
    h.record_source as hub_record_source,
   -- sl.record_source as sat_record_source,
    current_timestamp as dim_load_ts
from hub_lineitem h
join sat_latest sl
    on h.lineitem_pk = sl.lineitem_pk
where sl.rn = 1
