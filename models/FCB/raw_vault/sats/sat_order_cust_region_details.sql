{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'CUSTOMER_PK'
src_hashdiff: 'CUSTOMER_REGION_HASHDIFF'
src_payload:
    - REGION_PK
    - CUSTOMER_REGION_NAME
    - CUSTOMER_REGION_COMMENT
    - EFFECTIVE_FROM    
source_model: 'v_stg_orders'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
