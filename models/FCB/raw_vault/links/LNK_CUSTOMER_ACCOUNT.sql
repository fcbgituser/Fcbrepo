{{ config(materialized='incremental')    }}

{%- set source_model = "V_STG_FCB_ACCOUNT" -%}
{%- set src_pk = "CUSTOMER_ACCOUNT_HK"          -%}
{%- set src_fk = [ "ACCNT_HK","CUST_HK", "CTRY_RK","ACCNT_BK", "CUST_BK"]         -%} 
{%- set src_ldts = "LOAD_RK"      -%}
{%- set src_source = "SRC_RK"    -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}