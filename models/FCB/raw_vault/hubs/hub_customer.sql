{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'customer_pk'
business_keys: 
    - CUSTOMERKEY
source_models:
    - name: v_stg_orders
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}