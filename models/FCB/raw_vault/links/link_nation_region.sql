{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'NATION_REGION_PK'
foreign_hashkeys: 
    - 'NATION_PK'
    - 'REGION_PK'
source_models:
    - name: v_stg_orders
      rsrc_static: 'RAW_ORDERS'
    - name: v_stg_inventory
      rsrc_static: 'RAW_INVENTORY'
      link_hk: 'NATION_REGION_PK'
      fk_columns: 
          - NATION_PK
          - REGION_PK
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
