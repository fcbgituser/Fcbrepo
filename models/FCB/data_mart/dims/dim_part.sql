{{ config( materialized="table" ) }}

with sat_latest as (
    select
        s.part_pk,
        s.part_name,
        s.part_mfgr,
        s.part_brand,
        s.part_type,
        s.part_size,
        s.part_container,
        s.part_retailprice,
        s.part_comment,
        s.effective_from,
        row_number() over (
            partition by s.part_pk
            order by s.effective_from desc, s.load_date desc
        ) as rn
    from {{ ref("sat_inv_part_details") }} s
)

select 
    -- dim hashkey (surrogate key)
    md5_binary(h.part_pk) as part_hk,

    -- business key
    h.partkey as part_id,

    -- descriptive attributes
    sl.part_name,
    sl.part_mfgr,
    sl.part_brand,
    sl.part_type,
    sl.part_size,
    sl.part_container,
    sl.part_retailprice,
    sl.part_comment,

    -- audit fields
    sl.effective_from,
    h.record_source as hub_record_source,
    -- sl.record_source as sat_record_source,  -- optional if needed
    current_timestamp as dim_load_ts

from {{ ref("hub_part") }} h
join sat_latest sl
    on h.part_pk = sl.part_pk
where sl.rn = 1
