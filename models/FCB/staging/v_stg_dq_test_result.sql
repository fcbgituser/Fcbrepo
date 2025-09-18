{{ config(materialized="view")}}

with src_test as (select * from SAT_ACCOUNT_DETAILS where 1=1)
select * from src_test

{% set column_checks = {
    "ACCNT_ID": ["not_null", "unique"],
    "ACCNT_TYPE": {"accepted_values": ["'Checking'", "'Saving'"]},
    "ACCNT_HK::TEXT": {"referential_integrity": {
        "parent_table": "HUB_ACCOUNT",
        "parent_column": "ACCNT_HK::TEXT"
    }}
} %}

{{ store_data_quality_results(ref('SAT_ACCOUNT_DETAILS'), column_checks) }}
