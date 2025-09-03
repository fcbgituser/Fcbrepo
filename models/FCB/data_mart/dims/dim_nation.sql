{{ config(
    materialized='table',
    tags=['dim']
) }}

with hub as (
    select
        nation_pk,
        nation_key,
        load_date,
        record_source
    from {{ ref('hub_nation') }}
),

-- Get latest customer nation details
sat_cust as (
    select
        s.customer_pk,
        s.nation_pk,
        s.customer_nation_name,
        s.customer_nation_comment,
        s.effective_from,
        row_number() over (
            partition by s.customer_pk, s.nation_pk
            order by s.effective_from desc, s.load_date desc
        ) as rn
    from {{ ref('sat_order_cust_nation_details') }} s
),
latest_cust as (
    select *
    from sat_cust
    where rn = 1
),

-- Get latest supplier nation details
sat_supp as (
    select
        s.supplier_pk,
        s.supplier_nation_name,
        s.supplier_nation_comment,
        s.effective_from,
        row_number() over (
            partition by s.supplier_pk
            order by s.effective_from desc, s.load_date desc
        ) as rn
    from {{ ref('sat_inv_supp_nation_details') }} s
),
latest_supp as (
    select *
    from sat_supp
    where rn = 1
)

select
    md5_binary(h.nation_pk) nation_hk,
    h.nation_pk,
    h.nation_key,
    lc.customer_pk,
    lc.customer_nation_name,
    lc.customer_nation_comment,
    ls.supplier_pk,
    ls.supplier_nation_name,
    ls.supplier_nation_comment,
    greatest(
        coalesce(lc.effective_from, '1900-01-01'),
        coalesce(ls.effective_from, '1900-01-01')
    ) as effective_from,
    current_timestamp as load_date,
    'dim_build' as record_source
from hub h
left join latest_cust lc
    on h.nation_pk = lc.customer_pk
left join latest_supp ls
    on h.nation_pk = ls.supplier_pk
