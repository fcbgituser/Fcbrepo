{{ config(materialized="table") }}

with lineitem as (
    select
        md5_binary(h.lineitem_pk) as lineitem_hk,
        l.lineitem_pk,
        l.linenumber,
        l.orderkey,
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
        s.effective_from as lineitem_effective_from,
        s.load_date as lineitem_load_date,
        s.record_source as lineitem_record_source
    from {{ ref('hub_lineitem') }} l
    join (
        select *,
               row_number() over (
                   partition by lineitem_pk
                   order by effective_from desc, load_date desc
               ) as rn
        from {{ ref('sat_order_lineitem_details') }}
    ) s on l.lineitem_pk = s.lineitem_pk
    where s.rn = 1
),

orders as (
    select
        md5_binary(o.order_pk) order_hk,
        o.order_pk,
        o.orderkey,
        s.orderstatus,
        s.totalprice,
        s.orderdate,
        s.orderpriority,
        s.clerk,
        s.shippriority,
        s.order_comment,
        s.effective_from as order_effective_from,
        s.load_date as order_load_date,
        s.record_source as order_record_source
    from {{ ref('hub_order') }} o
    join (
        select *,
               row_number() over (
                   partition by order_pk
                   order by effective_from desc, load_date desc
               ) as rn
        from {{ ref('sat_order_order_details') }}
    ) s on o.order_pk = s.order_pk
    where s.rn = 1
),

customers as (
    select
        md5_binary(c.customer_pk) customer_hk,
        c.customer_pk,
        c.customerkey,
        s.customer_name,
        s.customer_address,
        s.customer_phone,
        s.customer_accbal,
        s.customer_mktsegment,
        s.customer_comment
    from {{ ref('hub_customer') }} c
    join (
        select *,
               row_number() over (
                   partition by customer_pk
                   order by effective_from desc, load_date desc
               ) as rn
        from {{ ref('sat_order_customer_details') }}
    ) s on c.customer_pk = s.customer_pk
    where s.rn = 1
),

suppliers as (
    select
        md5_binary(s.supplier_pk) supplier_hk,
        s.supplier_pk,
        s.supplierkey,
        sat.supplier_name,
        sat.supplier_address,
        sat.supplier_phone,
        sat.supplier_acctbal,
        sat.supplier_comment
    from {{ ref('hub_supplier') }} s
    join (
        select *,
               row_number() over (
                   partition by supplier_pk
                   order by effective_from desc, load_date desc
               ) as rn
        from {{ ref('sat_inv_supplier_details') }}
    ) sat on s.supplier_pk = sat.supplier_pk
    where sat.rn = 1
),

parts as (
    select
        md5_binary(p.part_pk) part_hk,
        p.part_pk,
        p.partkey,
        s.part_name,
        s.part_mfgr,
        s.part_brand,
        s.part_type,
        s.part_size,
        s.part_container,
        s.part_retailprice,
        s.part_comment
    from {{ ref('hub_part') }} p
    join (
        select *,
               row_number() over (
                   partition by part_pk
                   order by effective_from desc, load_date desc
               ) as rn
        from {{ ref('sat_inv_part_details') }}
    ) s on p.part_pk = s.part_pk
    where s.rn = 1
)

select
    li.lineitem_hk,
    o.order_hk,
    c.customer_hk,
    sup.supplier_hk,
    p.part_hk,
    li.lineitem_pk,
    li.linenumber,
    li.orderkey,
    li.extendedprice,
    li.quantity,
    li.discount,
    li.tax,
    li.shipdate,
    o.order_pk,
    o.orderdate,
    o.orderstatus,
    o.totalprice,
    c.customer_pk,
    c.customer_name,
    c.customer_mktsegment,
    sup.supplier_pk,
    sup.supplier_name,
    p.part_pk,
    p.part_name,
    p.part_type,
    p.part_brand
from lineitem li
join orders o 
    on li.orderkey = o.orderkey
join {{ ref('link_order_lineitem') }} lo 
    on li.lineitem_pk = lo.lineitem_pk 
   and o.order_pk = lo.order_pk
join {{ ref('link_customer_order') }} co 
    on o.order_pk = co.order_pk
join customers c 
    on co.customer_pk = c.customer_pk
join {{ ref('link_inventory_allocation') }} inv_alloc
    on li.lineitem_pk = inv_alloc.lineitem_pk
join suppliers sup 
    on inv_alloc.supplier_pk = sup.supplier_pk
join parts p 
    on inv_alloc.part_pk = p.part_pk
