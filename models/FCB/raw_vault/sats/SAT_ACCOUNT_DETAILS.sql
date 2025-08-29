{{ config(materialized="incremental") }}

{%- set yaml_metadata -%}

source_model: "V_STG_FCB_ACCOUNT"
src_pk: "ACCNT_HK"
src_hashdiff:
  source_column: "SAT_ACCNT_HASHDIFF"
  alias: "ROW_HSH"
src_payload:
  - "ACCNT_ID"
  - "ACCNT_NUM"
  - "CUST_ID"
  - "ACCNT_TYPE" 
  - "ACCNT_STATUS"
  - "LAST_MOD_DTS"
src_extra_not_null_columns:
  - "ACCNT_BK"
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
 
 