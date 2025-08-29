{% macro store_data_quality_results(model, column_checks) %}

    {% set model_name = model.identifier %}
    {% set model_schema = model.schema %}
    {% set run_time = modules.datetime.datetime.now().isoformat() %}

    {% for column, checks in column_checks.items() %}

        -- ========== NOT NULL check ==========
        {% if 'not_null' in checks %}
            {% set dq_query %}
                select '{{ model_schema }}.{{ model_name }}' as table_name,
                       '{{ column }}' as column_name,
                       'not_null' as check_type,
                       {{ column }} as failed_value,
                       current_timestamp as run_at
                from {{ model }}
                where {{ column }} is null
            {% endset %}

            {% set sql %}
                insert into dq_results (table_name, column_name, check_type, failed_value, run_at)
                {{ dq_query }}
            {% endset %}
            {% do run_query(sql) %}

            {% set sql %}
                insert into dq_summary (table_name, column_name, check_type, total_count, failed_count, status, run_at)
                select '{{ model_schema }}.{{ model_name }}',
                       '{{ column }}',
                       'not_null',
                       (select count(*) from {{ model }}),
                       (select count(*) from {{ model }} where {{ column }} is null),
                       case when (select count(*) from {{ model }} where {{ column }} is null) = 0 then true else false end,
                       current_timestamp
            {% endset %}
            {% do run_query(sql) %}
        {% endif %}

        -- ========== UNIQUE check ==========
        {% if 'unique' in checks %}
            {% set dq_query %}
                select '{{ model_schema }}.{{ model_name }}' as table_name,
                       '{{ column }}' as column_name,
                       'unique' as check_type,
                       {{ column }} as failed_value,
                       current_timestamp as run_at
                from (
                    select {{ column }}
                    from {{ model }}
                    group by {{ column }}
                    having count(*) > 1
                )
            {% endset %}

            {% set sql %}
                insert into dq_results (table_name, column_name, check_type, failed_value, run_at)
                {{ dq_query }}
            {% endset %}
            {% do run_query(sql) %}

            {% set sql %}
                insert into dq_summary (table_name, column_name, check_type, total_count, failed_count, status, run_at)
                select '{{ model_schema }}.{{ model_name }}',
                       '{{ column }}',
                       'unique',
                       (select count(*) from {{ model }}),
                       (select count(*) - count(distinct {{ column }}) from {{ model }}),
                       case when (select count(*) - count(distinct {{ column }}) from {{ model }}) = 0 then true else false end,
                       current_timestamp
            {% endset %}
            {% do run_query(sql) %}
        {% endif %}

        -- ========== ACCEPTED VALUES check ==========
        {% if 'accepted_values' in checks %}
            {% set accepted_values = checks['accepted_values'] %}
            {% set accepted_values_str = accepted_values | join(", ") %}

            {% set dq_query %}
                select '{{ model_schema }}.{{ model_name }}' as table_name,
                       '{{ column }}' as column_name,
                       'accepted_values' as check_type,
                       {{ column }} as failed_value,
                       current_timestamp as run_at
                from {{ model }}
                where {{ column }} not in ({{ accepted_values_str }})
                  and {{ column }} is not null
            {% endset %}

            {% set sql %}
                insert into dq_results (table_name, column_name, check_type, failed_value, run_at)
                {{ dq_query }}
            {% endset %}
            {% do run_query(sql) %}

            {% set sql %}
                insert into dq_summary (table_name, column_name, check_type, total_count, failed_count, status, run_at)
                select '{{ model_schema }}.{{ model_name }}',
                       '{{ column }}',
                       'accepted_values',
                       (select count(*) from {{ model }}),
                       (select count(*) from {{ model }} where {{ column }} not in ({{ accepted_values_str }})),
                       case when (select count(*) from {{ model }} where {{ column }} not in ({{ accepted_values_str }})) = 0 then true else false end,
                       current_timestamp
            {% endset %}
            {% do run_query(sql) %}
        {% endif %}

        -- ========== REFERENTIAL INTEGRITY check ==========
        {% if 'referential_integrity' in checks %}
            {% set parent_table = checks['referential_integrity']['parent_table'] %}
            {% set parent_column = checks['referential_integrity']['parent_column'] %}

            {% set dq_query %}
                select '{{ model_schema }}.{{ model_name }}' as table_name,
                       '{{ column }}' as column_name,
                       'referential_integrity' as check_type,
                       c.{{ column }} as failed_value,
                       current_timestamp as run_at
                from {{ model }} c
                left join {{ parent_table }} p
                    on c.{{ column }} = p.{{ parent_column }}
                where p.{{ parent_column }} is null
                  and c.{{ column }} is not null
            {% endset %}

            {% set sql %}
                insert into dq_results (table_name, column_name, check_type, failed_value, run_at)
                {{ dq_query }}
            {% endset %}
            {% do run_query(sql) %}

            {% set sql %}
                insert into dq_summary (table_name, column_name, check_type, total_count, failed_count, status, run_at)
                select '{{ model_schema }}.{{ model_name }}',
                       '{{ column }}',
                       'referential_integrity',
                       (select count(*) from {{ model }}),
                       (select count(*) from {{ model }} c left join {{ parent_table }} p 
                          on c.{{ column }} = p.{{ parent_column }}
                        where p.{{ parent_column }} is null and c.{{ column }} is not null),
                       case when (select count(*) from {{ model }} c left join {{ parent_table }} p 
                          on c.{{ column }} = p.{{ parent_column }}
                        where p.{{ parent_column }} is null and c.{{ column }} is not null) = 0 then true else false end,
                       current_timestamp
            {% endset %}
            {% do run_query(sql) %}
        {% endif %}

    {% endfor %}
{% endmacro %}
