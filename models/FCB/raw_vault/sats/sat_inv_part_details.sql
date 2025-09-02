{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'PART_PK'
src_hashdiff: 'PART_HASHDIFF'
src_payload:
    - PART_NAME
    - PART_MFGR
    - PART_BRAND
    - PART_TYPE
    - PART_SIZE
    - PART_CONTAINER
    - PART_RETAILPRICE
    - PART_COMMENT
    - EFFECTIVE_FROM    
source_model: 'v_stg_inventory'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
