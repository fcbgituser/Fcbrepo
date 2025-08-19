{{ config( materialized="view" )}}
{%- set yaml_metadata -%}
source_model: "V_PSTG_FCB_RAW_CUSTOMERS"
hashed_columns:
    CUST_HK:
        - "CUST_BK"
    SAT_CUSTOMER_HASHDIFF:
        is_hasfdiff: true
        columns:
        - "CUST_BK"
        - "FIRST_NM"
        - "LAST_NM"
        - "CUST_EMAIL" 
        - "LAST_MOD_DTS"
        - "LOAD_RK"
        - "CTRY_RK"
        - "SRC_RK" 

derived_column :
    CUST_BK: "CUSTOMER_ID::TEXT"
    CUST_ID: "CUSTOMER_ID::TEXT"
    FIRST_NM: "FIRST_NAME::TEXT"
    LAST_NM: "LAST_NAME::TEXT"
    CUST_EMAIL: "EMAIL::TEXT"
    LAST_MOD_DTS: "CREATED_AT::TIMESTAMP_TZ"
    LOAD_TS: "CREATE_DT::TIMESTAMP_TZ"
    LOAD_RK: "CONCAT(CREATED_AT||'-'||CUSTOMER_ID)::TEXT"
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