{{ config( materialized="view" )}}
{%- set yaml_metadata -%}
source_model: "V_PSTG_FCB_RAW_ACCOUNTS"
hashed_columns:
    ACCNT_HK:
        - "ACCNT_BK"
    SAT_ACCNT_HASHDIFF:
        is_hasfdiff: true
        columns:
        - "ACCNT_BK"
        - "ACCNT_ID"
        - "CUST_ID"
        - "ACCNT_TYPE" 
        - "ACCNT_STATUS"
        - "LAST_MOD_DTS"
        - "LOAD_RK"
        - "CTRY_RK"
        - "SRC_RK" 

derived_column :
    ACCNT_BK: "ACCOUNT_NUMBER::NUMBER"
    ACCNT_ID: "ACCOUNT_ID::TEXT"
    ACCNT_NO: "ACCOUNT_NUMBER"
    CUST_ID: "CUSTOMER_ID::TEXT"
    ACCNT_TYPE: "ACCOUNT_TYPE::TEXT"
    ACCNT_STATUS: "ACCOUNT_TYPE::TEXT"
    LAST_MOD_DTS: "CREATED_AT::TIMESTAMP_TZ"
    LOAD_TS: "CREATE_DT::TIMESTAMP_TZ"
    LOAD_RK: "CONCAT(CREATED_AT,ACCOUNT_ID)::TEXT"
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