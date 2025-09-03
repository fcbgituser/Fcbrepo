{{ config( materialized="incremental" )}}

{%- set source_model = "V_STG_FCB_CUSTOMER" -%}
{%- set src_pk = "CUST_HK" -%}
{%- set src_nk = ["CUST_BK","LOAD_TS"]  -%}
{%- set src_extra_columns = "SRC_RK" -%}
{%- set src_ldts = "LOAD_RK" -%}

{{ dbtvault.hub(src_pk = src_pk, src_nk = src_nk, src_extra_columns = src_extra_columns, 
src_ldts = src_ldts, src_source = src_source, source_model = source_model) }}
