-- A simple macro to standardize the load date/timestamp column name.
-- In a real-world scenario, this might use a more complex logic
-- to get the actual load timestamp from the source system.
{% macro get_dbtvault_load_dts() %}
    created_at as load_dts
{% endmacro %}
