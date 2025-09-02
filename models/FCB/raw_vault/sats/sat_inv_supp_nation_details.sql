{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'SUPPLIER_PK'
src_hashdiff: 'SUPPLIER_NATION_HASHDIFF'
src_payload:
    - SUPPLIER_NATION_NAME
    - SUPPLIER_NATION_COMMENT
    - EFFECTIVE_FROM
source_model: 'v_stg_inventory'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
