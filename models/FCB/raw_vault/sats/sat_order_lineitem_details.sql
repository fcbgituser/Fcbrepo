{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'LINEITEM_PK'
src_hashdiff: 'LINEITEM_HASHDIFF'
src_payload:
    - COMMITDATE
    - DISCOUNT
    - EXTENDEDPRICE
    - LINE_COMMENT
    - QUANTITY
    - RECEIPTDATE
    - RETURNFLAG
    - SHIPDATE
    - SHIPINSTRUCT
    - SHIPMODE
    - TAX
    - EFFECTIVE_FROM    
source_model: 'v_stg_orders'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
