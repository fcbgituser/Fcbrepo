{{ config(materialized="view") }}

with
    src_accnt as (
        select *, {{ get_dbtvault_load_dts() }} from {{ source("FCB", "RAW_ACCOUNTS") }}
    )
select
    account_id, account_number, customer_id, account_type, status, created_at, create_dt
from src_accnt
