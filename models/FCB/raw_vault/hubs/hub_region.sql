{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'REGION_PK'
business_keys: 
    - REGION_KEY
source_models:
    - name: v_stg_orders
      rsrc_static: 'RAW_ORDER'
    - name: v_stg_inventory
      hk_column: 'REGION_PK'
      bk_columns:
          - REGION_KEY
      rsrc_static: 'RAW_INVENTORY'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}