{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'ORDER_PK'
src_hashdiff: 'ORDER_HASHDIFF'
src_payload:
    - ORDERSTATUS
    - TOTALPRICE
    - ORDERDATE
    - ORDERPRIORITY
    - CLERK
    - SHIPPRIORITY
    - ORDER_COMMENT
    - EFFECTIVE_FROM    
source_model: 'v_stg_orders'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
