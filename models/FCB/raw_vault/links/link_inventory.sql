{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'INVENTORY_PK'
foreign_hashkeys: 
    - 'SUPPLIER_PK'
    - 'PART_PK'
source_models: v_stg_inventory
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata)}}
