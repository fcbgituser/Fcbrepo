-- A macro to consistently generate hash keys (hkey) for hubs and links.
-- It takes a list of columns to be concatenated and hashed.
{% macro get_dbtvault_hkey(columns) %}
    md5(
        {% for column in columns %}
        cast({{ column }} as varchar)
        {{ '||' if not loop.last }}
        {% endfor %}
    )
{% endmacro %}
 