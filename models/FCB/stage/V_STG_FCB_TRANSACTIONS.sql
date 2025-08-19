{{ config( materialized="view" )}}
{%- set yaml_metadata -%}
source_model: "V_PSTG_FCB_RAW_TRANSACTIONS"
hashed_columns:
    TRANSACTION_HK:
        - "TRANSACTION_BK"
    SAT_TRANSACTIONS_HASHDIFF:
        is_hasfdiff: true
        columns:
        - "TRANSACTION_BK"
        - "ACCNT_ID"
        - "TRX_AMT" 
        - "DESCR"
        - "LAST_MOD_DTS"
        - "LOAD_RK"
        - "CTRY_RK"
        - "SRC_RK" 

derived_column :
    TRANSACTION_BK: "TRANSACTION_ID::TEXT"
    TRX_ID: "TRANSACTION_ID::TEXT"
    ACCNT_ID: "ACCOUNT_ID::TEXT"
    TRX_AMT: "AMOUNT::NUMBER"
    DESCR: "DESCRIPTION::TEXT"
    LAST_MOD_DTS: "TRANSACTION_DATE::TIMESTAMP_TZ"
    LOAD_TS: "CREATE_DT::TIMESTAMP_TZ"
    LOAD_RK: "CONCAT(TRANSACTION_DATE,ACCOUNT_ID)::TEXT"
    SRC_RK: "{{var('SOURCE_RK')}}"
    CTRY_RK: "{{var('GLOBAL_CTRY')}}"
    BK_RK: "1"
{%- endset -%}


{% set metadata_dict = fromyaml(yaml_metadata) %}

{{
    dbtvault.stage(
        include_source_columns=false,
        source_model=metadata_dict["source_model"],
        derived_columns=metadata_dict["derived_column"],
        null_columns=none,
        hashed_columns=metadata_dict["hashed_columns"],
        ranked_columns=none, 
    
    )
}}