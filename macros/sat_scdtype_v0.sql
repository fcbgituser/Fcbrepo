{# macros/datavault4dbt/sat_v0.sql
   Overrides datavault4dbt.sat_v0 in the project (no dot in macro name).
   Implements SCD Type 2 for Snowflake via MERGE.
   Usage:
     {% set yaml_metadata = {
       'source_model': 'stg_customers',
       'target_relation': ref('raw_sat_customer'),
       'hub_key': 'customer_hk',
       'hashdiff_col': 'hashdiff',
       'load_date_col': 'load_ts',
       'record_source_col': 'record_source'
     } %}
     {{ sat_v0(yaml_metadata=yaml_metadata) }}
#}

{% macro sat_v0(yaml_metadata, open_ended_ts=None) -%}

{%- set ym = yaml_metadata -%}
{%- if ym is none -%}
  {{ exceptions.raise_compiler_error("sat_v0: yaml_metadata is required.") }}
{%- endif -%}

{# extract values with defaults #}
{%- set source_model = ym.get('source_model') if ym.get('source_model') is not none else None -%}
{%- set target_relation = ym.get('target_relation') if ym.get('target_relation') is not none else None -%}
{%- set hub_key = ym.get('hub_key') if ym.get('hub_key') is not none else 'hub_hk' -%}
{%- set hashdiff_col = ym.get('hashdiff_col') if ym.get('hashdiff_col') is not none else 'hashdiff' -%}
{%- set load_date_col = ym.get('load_date_col') if ym.get('load_date_col') is not none else 'load_date' -%}
{%- set record_source_col = ym.get('record_source_col') if ym.get('record_source_col') is not none else 'record_source' -%}
{%- set open_ts = open_ended_ts if open_ended_ts is not none else (ym.get('open_ended_ts') if ym.get('open_ended_ts') is not none else '9999-12-31 23:59:59') -%}

{%- if source_model is none -%}
  {{ exceptions.raise_compiler_error("sat_v0: 'source_model' must be set in yaml_metadata (e.g. 'stg_customers').") }}
{%- endif -%}
{%- if target_relation is none -%}
  {{ exceptions.raise_compiler_error("sat_v0: 'target_relation' must be set in yaml_metadata (e.g. ref('raw_sat_customer')).") }}
{%- endif -%}

{# target may be a ref(...) result or a string; use it directly in the rendered SQL #}
{%- set target = target_relation -%}

with staged as (
  select
    src.{{ hub_key }} as {{ hub_key }},
    src.{{ hashdiff_col }} as {{ hashdiff_col }},
    cast(src.{{ load_date_col }} as timestamp_ntz) as effective_from,
    cast('{{ open_ts }}' as timestamp_ntz) as effective_to,
    true as is_current,
    src.{{ record_source_col }} as {{ record_source_col }},
    src.*
  from {{ ref(source_model) }} as src
)

merge into {{ target }} as target
using (
  select * from staged
) as src
on target.{{ hub_key }} = src.{{ hub_key }}
and target.is_current = true

when matched and target.{{ hashdiff_col }} <> src.{{ hashdiff_col }} then
  update set
    target.effective_to = dateadd(second, -1, src.effective_from),
    target.is_current = false

when not matched then
  insert (
    {{ hub_key }},
    {{ hashdiff_col }},
    effective_from,
    effective_to,
    is_current,
    {{ record_source_col }}
  )
  values (
    src.{{ hub_key }},
    src.{{ hashdiff_col }},
    src.effective_from,
    src.effective_to,
    src.is_current,
    src.{{ record_source_col }}
  );

{%- endmacro %}
