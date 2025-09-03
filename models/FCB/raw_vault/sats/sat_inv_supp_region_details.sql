{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'SUPPLIER_PK'
src_hashdiff: 'SUPPLIER_REGION_HASHDIFF'
src_payload:
    - SUPPLIER_REGION_NAME
    - SUPPLIER_REGION_COMMENT
    - EFFECTIVE_FROM
source_model: 'v_stg_inventory'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}