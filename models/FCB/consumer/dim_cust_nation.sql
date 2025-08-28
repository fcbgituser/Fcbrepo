{{ config( materialized="table" )}}
with sat_latest as (
    select
        s.customer_pk,
        s.customer_nation_name,
        s.customer_nation_comment,
        s.effective_from,
        row_number() over (
            partition by s.customer_pk
            order by s.effective_from desc, s.load_date desc
        ) as rn
    from sat_order_cust_nation_details s
)
select 
    -- dim hash key
md5_binary(h.nation_pk) as nation_sk,
    
    -- Business key from hub
    h.nation_key,
    
    -- Attributes from satellite
    sl.customer_nation_name,
    sl.customer_nation_comment,
    
    -- Audit info (optional)
    sl.effective_from,
    h.record_source as hub_record_source,
    --sl.record_source as sat_record_source,
    current_timestamp as dim_load_ts
from hub_nation h
join sat_latest sl
    on h.nation_pk = sl.customer_pk   -- HUB_NATION.NATION_PK = SAT_ORDER_CUST_NATION_DETAILS.CUSTOMER_PK
where sl.rn = 1;
