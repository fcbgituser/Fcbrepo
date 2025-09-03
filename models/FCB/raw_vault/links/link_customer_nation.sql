{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'LINK_CUSTOMER_NATION_PK'
foreign_hashkeys: 
    - 'CUSTOMER_PK'
    - 'NATION_PK'
source_models: v_stg_orders
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
