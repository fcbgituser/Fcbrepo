-- macros/data_quality_checks.sql
{% macro data_quality_checks(model, column_checks) %}
    {% set results = [] %}
    {% for column, checks in column_checks.items() %}

        -- NOT NULL check
        {% if 'not_null' in checks %}
            {% set not_null_query %}
                select count(*) as null_count 
                from {{ model }}
                where {{ column }} is null
            {% endset %}
            {% set not_null_result = run_query(not_null_query) %}
            {% do results.append({
                'check': 'not_null',
                'column': column,
                'result': (not_null_result.columns[0].values()[0] == 0)
            }) %}
        {% endif %}

        -- NIQUE check
        {% if 'unique' in checks %}
            {% set unique_query %}
                select count(*) - count(distinct {{ column }}) as duplicate_count
                from {{ model }}
            {% endset %}
            {% set unique_result = run_query(unique_query) %}
            {% do results.append({
                'check': 'unique',
                'column': column,
                'result': (unique_result.columns[0].values()[0] == 0)
            }) %}
        {% endif %}
        --  ACCEPTED VALUES check
        {% if 'accepted_values' in checks %}
            {% set accepted_values = checks['accepted_values'] %}
            {% set accepted_values_str = accepted_values | join(", ") %}
            {% set accepted_query %}
                select count(*) as invalid_count 
                from {{ model }}
                where {{ column }} not in ({{ accepted_values_str }})
            {% endset %}
            {% set accepted_result = run_query(accepted_query) %}
            {% do results.append({
                'check': 'accepted_values',
                'column': column,
                'result': (accepted_result.columns[0].values()[0] == 0)
            }) %}
        {% endif %} 
        -- REFERENTIAL INTEGRITY check
        {% if 'referential_integrity' in checks %}
            {% set parent_table = checks['referential_integrity']['parent_table'] %}
            {% set parent_column = checks['referential_integrity']['parent_column'] %}
            {% set ref_int_query %}
                select count(*) as invalid_fk_count
                from {{ model }} c
                left join {{ parent_table }} p
                    on c.{{ column }} = p.{{ parent_column }}
                where p.{{ parent_column }} is null
                and c.{{ column }} is not null
            {% endset %}
            {% set ref_int_result = run_query(ref_int_query) %}
            {% do results.append({
                'check': 'referential_integrity',
                'column': column,
                'result': (ref_int_result.columns[0].values()[0] == 0)
            }) %}
        {% endif %}

    {% endfor %}
    {{ log("✅ Data Quality Check Results: " ~ results | tojson, info=True) }}
    {% do return(results) %}
{% endmacro %}
