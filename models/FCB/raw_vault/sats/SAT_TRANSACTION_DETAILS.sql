{{ config(materialized="incremental") }}

{%- set yaml_metadata -%}

source_model: "V_STG_FCB_TRANSACTIONS"
src_pk: "TRANSACTION_HK"
src_hashdiff:
  source_column: "SAT_TRANSACTIONS_HASHDIFF"
  alias: "ROW_HSH"
src_payload:
  - "ACCNT_ID"
  - "TRX_AMT" 
  - "DESCR"
  - "LAST_MOD_DTS"
src_extra_not_null_columns:
  - "TRANSACTION_BK"
src_ldts: "LOAD_RK"
src_source: "SRC_RK"

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{
    dbtvault.sat(
        src_pk=metadata_dict["src_pk"],
        src_hashdiff=metadata_dict["src_hashdiff"],
        src_payload=metadata_dict["src_payload"],
        src_extra_columns=metadata_dict["src_extra_not_null_columns"],
        src_eff=metadata_dict["src_eff"],
        src_ldts=metadata_dict["src_ldts"],
        src_source=metadata_dict["src_source"],
        source_model=metadata_dict["source_model"],
    )
}}
 
 