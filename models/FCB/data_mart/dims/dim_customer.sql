{{ config(
    materialized='table',
    tags=['dim']
) }}
with sat_latest as (
    select
        s.customer_pk,
        s.customer_name,
        s.customer_address,
        s.customer_phone,
        s.customer_accbal,
        s.customer_mktsegment,
        s.customer_comment,
        s.effective_from,
        row_number() over (
            partition by s.customer_pk 
            order by s.effective_from desc, s.load_date desc
        ) as rn
    from {{ref("sat_order_customer_details")}} s
)
    select 
    -- dim hashkey
    md5_binary( h.customer_pk) as customer_hk,
    
    -- business key
    h.customerkey as customer_id,
        -- descriptive attributes
    sl.customer_name,
    sl.customer_address,
    sl.customer_phone,
    sl.customer_accbal,
    sl.customer_mktsegment,
    sl.customer_comment,
        -- audit fields (optional)
    sl.effective_from,
    h.record_source as hub_record_source,
   -- sl.record_source as sat_record_source,
    current_timestamp as dim_load_ts
    from {{ref("hub_customer")}} h
join sat_latest sl
    on h.customer_pk = sl.customer_pk
where sl.rn = 1