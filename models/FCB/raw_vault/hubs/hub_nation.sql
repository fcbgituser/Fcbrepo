{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'NATION_PK'
business_keys: 
    - NATION_KEY
source_models:
    - name: v_stg_orders
      rsrc_static: 'RAW_ORDER'
    - name: v_stg_inventory
      hk_column: 'NATION_PK'
      bk_columns:
          - NATION_KEY
      rsrc_static: 'RAW_INVENTORY'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}