{{ config(materialized='view')}}

with _select as(
select * from TEST_FCB.INFORMATION_SCHEMA.APPLICABLE_ROLES 
)
select * from _select
