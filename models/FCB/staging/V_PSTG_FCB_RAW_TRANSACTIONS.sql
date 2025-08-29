{{ config(materialized="view") }}

with
    src_trx as (
        select *, {{ get_dbtvault_load_dts() }}
        from {{ source("FCB", "RAW_TRANSACTIONS") }}
    )
select transaction_id, account_id, amount, description, transaction_date, create_dt
from src_trx
