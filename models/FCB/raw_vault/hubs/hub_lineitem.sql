{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'LINEITEM_PK'
business_keys: 
    - LINENUMBER
    - ORDERKEY
source_models:
    - name: v_stg_orders
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}