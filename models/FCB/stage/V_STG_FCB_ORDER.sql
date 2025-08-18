{{ config( materialized="view" )}}
{%- set yaml_metadata -%}
source_model:
    FCB: "ORDERS_RAW"
hashed_columns:
    ORD_HK:
        - "ORD_BK"
    ROW_HASH:
        is_hasfdiff: true
        columns:
        - "ORD_ID"
        - "CUST_ID"
        - "PRODUCT_NO"
        - "ORD_DT"  
        - "QTY"
        - "AMOUNT"
        - "LOAD_RK"
        - "SRC_RK" 

derived_column :
    ORD_BK:
        - "ORDER_ID"
        - "CUSTOMER_ID"
    ORD_ID: "ORDER_ID"   
    CUST_ID: "CUSTOMER_ID"
    PRODUCT_NO: "PRODUCT_ID"
    ORD_DT: "ORDER_DATE"
    QTY: "QUANTITY"
    AMOUNT: "PRICE"
    LOAD_TS: "UPDATED_AT"
    LOAD_RK: "CONCAT(UPDATED_AT,ORDER_ID)"
    SRC_RK: "201301"
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