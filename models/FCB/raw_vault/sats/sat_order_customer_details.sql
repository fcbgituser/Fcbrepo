{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'CUSTOMER_PK'
src_hashdiff: 'CUSTOMER_HASHDIFF'
src_payload:
    - CUSTOMER_NAME
    - CUSTOMER_ADDRESS
    - CUSTOMER_PHONE
    - CUSTOMER_ACCBAL
    - CUSTOMER_MKTSEGMENT
    - CUSTOMER_COMMENT
    - EFFECTIVE_FROM    
source_model: 'v_stg_orders'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
