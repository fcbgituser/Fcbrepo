{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'PART_PK'
business_keys: 
    - PARTKEY
source_models:
    - name: v_stg_orders
      rsrc_static: 'RAW_ORDER'
    - name: v_stg_inventory
      hk_column: 'PART_PK'
      bk_columns:
          - PARTKEY
      rsrc_static: 'RAW_INVENTORY'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}