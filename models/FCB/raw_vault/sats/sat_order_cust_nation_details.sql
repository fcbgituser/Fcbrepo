{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'CUSTOMER_PK'
src_hashdiff: 'CUSTOMER_NATION_HASHDIFF'
src_payload:
    - NATION_PK
    - CUSTOMER_NATION_NAME
    - CUSTOMER_NATION_COMMENT
    - EFFECTIVE_FROM    
source_model: 'v_stg_orders'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}