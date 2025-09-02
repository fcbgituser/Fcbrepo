{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'INVENTORY_ALLOCATION_PK'
foreign_hashkeys: 
    - 'PART_PK'
    - 'SUPPLIER_PK'
    - 'LINEITEM_PK'
source_models: v_stg_orders
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata)}}
