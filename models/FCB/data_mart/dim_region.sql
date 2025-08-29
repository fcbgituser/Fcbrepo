
{{ config(
    materialized = "table"
) }}

with hub as (
    select
        REGION_PK,
        REGION_KEY
    from {{ ref('hub_region') }}
),

cust_latest as (
    select
        CUSTOMER_PK,
        CUSTOMER_REGION_NAME,
        CUSTOMER_REGION_COMMENT,
        row_number() over (
            partition by CUSTOMER_PK
            order by EFFECTIVE_FROM desc, LOAD_DATE desc
        ) as rn
    from {{ ref('sat_order_cust_region_details') }}
),
cust as (
    select *
    from cust_latest
    where rn = 1
),

supp_latest as (
    select
        SUPPLIER_PK,
        SUPPLIER_NATION_NAME,
        SUPPLIER_NATION_COMMENT,
        row_number() over (
            partition by SUPPLIER_PK
            order by EFFECTIVE_FROM desc, LOAD_DATE desc
        ) as rn
    from {{ ref('sat_inv_supp_region_details') }}
),
supp as (
    select *
    from supp_latest
    where rn = 1
)

select
    md5_binary(REGION_PK) REGION_HK,
    h.REGION_PK,
    h.REGION_KEY,
    c.CUSTOMER_REGION_NAME,
    c.CUSTOMER_REGION_COMMENT,
    s.SUPPLIER_NATION_NAME,
    s.SUPPLIER_NATION_COMMENT
from hub h
left join cust c
    on h.REGION_PK = c.CUSTOMER_PK
left join supp s
    on h.REGION_PK = s.SUPPLIER_PK
