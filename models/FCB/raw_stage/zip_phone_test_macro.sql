{{ config(
    materialized='view',
    tags=['raw']
) }} 


select {{phone_number_cleansing('2025550123')}} phone_number