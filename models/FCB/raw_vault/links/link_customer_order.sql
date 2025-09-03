{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'ORDER_CUSTOMER_PK'
foreign_hashkeys: 
    - 'CUSTOMER_PK'
    - 'ORDER_PK'
source_models: v_stg_orders
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
