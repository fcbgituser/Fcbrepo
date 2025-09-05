{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'INVENTORY_PK'
src_hashdiff: 'INVENTORY_HASHDIFF'
src_payload:
    - AVAILQTY
    - SUPPLYCOST
    - PART_SUPPLY_COMMENT
    - EFFECTIVE_FROM    
source_model: 'v_stg_inventory'
{%- endset -%}    

{{ snowflake__sat_v0(yaml_metadata=yaml_metadata) }}
