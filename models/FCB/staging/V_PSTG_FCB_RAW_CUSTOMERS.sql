{{ config(materialized="view") }}

with
    src_cust as (
        select *, {{ get_dbtvault_load_dts() }}
        from {{ source("FCB", "RAW_CUSTOMERS") }}
    )
select customer_id, first_name, last_name, email, created_at,create_dt
from src_cust
