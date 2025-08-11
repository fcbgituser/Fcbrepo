{{ config(materialized="incremental") }}

{%- set yaml_metadata -%}

source_model: "V_STG_FCB_ORDER"
src_pk: "ORD_HK"
src_hashdiff:
  source_column: "ROW_HASH"
  alias: "ROW_HSH"
src_payload:
  - "ORD_ID"
  - "CUST_ID"
  - "PRODUCT_NO"
  - "ORD_DT"  
  - "QTY"
  - "AMOUNT"
  - "LOAD_RK"
  - "SRC_RK"
 
 