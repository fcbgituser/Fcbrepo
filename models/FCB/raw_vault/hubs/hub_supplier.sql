{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'SUPPLIER_PK'
business_keys: 
    - SUPPLIERKEY
source_models:
    - name: v_stg_orders
      rsrc_static: 'RAW_ORDER'
    - name: v_stg_inventory
      hk_column: 'SUPPLIER_PK'
      bk_columns:
          - SUPPLIERKEY
      rsrc_static: 'RAW_INVENTORY'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}