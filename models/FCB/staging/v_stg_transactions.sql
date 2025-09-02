{{ config(
    materialized='view',
    tags=['stage']
) }}
{%- set yaml_metadata -%}
source_model: 'raw_transaction'
ldts:  TO_DATE('{{ var('load_date')}}')
rsrc: '!RAW_TRANSACTION'
derived_columns:
    load_date:
        value: 'DATEADD(DAY, 1, TRANSACTION_DATE)'
        datatype: 'date'
hashed_columns:
  TRANSACTION_PK:
    - 'CUSTOMER_ID'
    - 'TRANSACTION_NUMBER'
  CUSTOMER_PK: 'CUSTOMER_ID'
  ORDER_PK: 'ORDER_ID'
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata)  }}