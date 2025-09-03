{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'LINK_LINEITEM_ORDER_PK'
foreign_hashkeys: 
    - 'ORDER_PK'
    - 'LINEITEM_PK'
source_models: v_stg_orders
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata)}} 
