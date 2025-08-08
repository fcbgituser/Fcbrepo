{{ config(materialized="view") }}

with _select as (select * from test_fcb.information_schema.applicable_roles)
select *
from _select
