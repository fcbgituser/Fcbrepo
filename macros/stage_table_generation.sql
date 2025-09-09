-- macros/stage_table_generation_select.sql
{%- macro stage_table_generation_select(base_schema, base_model,
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
{%- for expr in select_list -%}
  {{ expr }}{{ "," if not loop.last }}
{%- endfor -%}
from {{ source(base_schema, base_model) }} as {{ base_alias }}

{%- for j in joins -%}
  {%- set join_type = j.get('type','left') | upper -%}
  {%- set j_alias = j.get('alias','j' ~ loop.index) -%}
  {%- set j_schema = j.get('src_schema') -%}
  {%- set j_model  = j.get('src_model') -%}
  {{ join_type }} JOIN {{ source(j_schema, j_model) }} AS {{ j_alias }}
    ON
    {%- set on_pairs = j.get('on', []) -%}
    {%- for pair in on_pairs -%}
      {{ pair[0] }} = {{ pair[1] }}{{ " AND" if not loop.last else "" }}
    {%- endfor -%}
{%- endfor -%}

{%- if where_clause %}
where {{ where_clause }}
{%- endif -%}

{%- if order_by %}
order by
  {%- for ob in order_by -%}
    {{ ob }}{{ "," if not loop.last }}
  {%- endfor -%}
{%- endif -%}
;
{%- endmacro -%}
