-- macros/stage_table_generation_select_fixed.sql
{%- macro raw_stage_table_generation(base_schema, base_model,
                                        select_list,
                                        joins=[],
                                        where_clause=None,
                                        order_by=None,
                                        base_alias='b') -%}
{#
  base_schema, base_model  : strings passed to source()
  select_list              : list of select expressions as strings (fully-qualified with aliases as you want them)
  joins                    : list of join dicts. Each join dict:
      {
        'src_schema': 'SCHEMA',
        'src_model' : 'MODEL',
        'alias'     : 'a',
        'type'      : 'left' (default 'left'),
        'on'        : [ ['a.L_ORDERKEY', 'b.O_ORDERKEY'], ['a.L_PARTKEY', 'g.P_PARTKEY'] ]
      }
    'on' is a list of pairs (left_expr, right_expr). Expressions are rendered directly (strings).
  where_clause             : optional raw SQL string (do NOT include the WHERE keyword)
  order_by                 : optional list of expressions for ORDER BY
  base_alias               : alias for base relation in FROM clause (default 'b')
#}

select
{% for expr in select_list %}
  {{ expr }}{% if not loop.last %},{% endif %}
{% endfor %}
from {{ source(base_schema, base_model) }} as {{ base_alias }}

{% for j in joins %}
  {# default join type = LEFT #}
  {% set join_type = (j.get('type') or 'left').upper() %}
  {% set j_alias = j.get('alias') or ('j' ~ loop.index) %}
  {% set j_schema = j.get('src_schema') %}
  {% set j_model  = j.get('src_model') %}
  {{ join_type }} JOIN {{ source(j_schema, j_model) }} AS {{ j_alias }}
    ON
    {% for pair in j.get('on', []) %}
      {{ pair[0] }} = {{ pair[1] }}{% if not loop.last %} AND{% endif %}
    {% endfor %}

{% endfor %}

{% if where_clause %}
where {{ where_clause }}
{% endif %}

{% if order_by %}
order by
{% for ob in order_by %}
  {{ ob }}{% if not loop.last %},{% endif %}
{% endfor %}
{% endif %}
{%- endmacro -%}
