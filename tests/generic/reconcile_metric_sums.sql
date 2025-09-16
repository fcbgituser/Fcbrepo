{% test reconcile_metric_sums(model) %}

{% set src = kwargs.get('source_relation') %}
{% set tgt_arg = kwargs.get('target_relation') %}
{% set metrics = kwargs.get('metric_cols', []) %}
{% set tol = kwargs.get('metric_tolerance_pct', 0.0) | float %}
{% set abs_thresh = kwargs.get('absolute_threshold', 0.0) | float %}

{% if not src %}
  {% do exceptions.raise_compiler_error("reconcile_metric_sums: 'source_relation' is required.") %}
{% endif %}
{% if metrics | length == 0 %}
  {% do exceptions.raise_compiler_error("reconcile_metric_sums: 'metric_cols' is required and must be a non-empty list.") %}
{% endif %}

-- choose target: explicit arg takes precedence, otherwise model under test
{% if tgt_arg %}
  {% set tgt = tgt_arg %}
{% else %}
  {% set tgt = model %}
{% endif %}

{% for m in metrics %}
  {% set m_safe = m %}
  {% if loop.first %}
with
  src_{{ loop.index }} as (
    select sum(coalesce({{ m_safe }},0)) as src_sum from {{ src }}
  ),
  tgt_{{ loop.index }} as (
    select sum(coalesce({{ m_safe }},0)) as tgt_sum from {{ tgt }}
  ),
  comp_{{ loop.index }} as (
    select
      '{{ m_safe }}' as metric,
      src_{{ loop.index }}.src_sum as src_sum,
      tgt_{{ loop.index }}.tgt_sum as tgt_sum,
      abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) as abs_diff,
      case
        when greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))) = 0 then 0
        else (abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) * 100.0 /
              greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))))
      end as pct_diff
    from src_{{ loop.index }} cross join tgt_{{ loop.index }}
    where
      abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) > {{ abs_thresh }}
      or (
        case
          when greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))) = 0 then 0
          else (abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) * 100.0 /
                greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))))
        end > {{ tol }}
      )
  )
  {% else %}
  ,
  src_{{ loop.index }} as (
    select sum(coalesce({{ m_safe }},0)) as src_sum from {{ src }}
  ),
  tgt_{{ loop.index }} as (
    select sum(coalesce({{ m_safe }},0)) as tgt_sum from {{ tgt }}
  ),
  comp_{{ loop.index }} as (
    select
      '{{ m_safe }}' as metric,
      src_{{ loop.index }}.src_sum as src_sum,
      tgt_{{ loop.index }}.tgt_sum as tgt_sum,
      abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) as abs_diff,
      case
        when greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))) = 0 then 0
        else (abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) * 100.0 /
              greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))))
      end as pct_diff
    from src_{{ loop.index }} cross join tgt_{{ loop.index }}
    where
      abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) > {{ abs_thresh }}
      or (
        case
          when greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))) = 0 then 0
          else (abs(src_{{ loop.index }}.src_sum - tgt_{{ loop.index }}.tgt_sum) * 100.0 /
                greatest(abs(coalesce(src_{{ loop.index }}.src_sum,0)), abs(coalesce(tgt_{{ loop.index }}.tgt_sum,0))))
        end > {{ tol }}
      )
  )
  {% endif %}
{% endfor %}

select *
from (
  {% for m in metrics %}
    select * from comp_{{ loop.index }} {% if not loop.last %} union all {% endif %}
  {% endfor %}
) failures
order by pct_diff desc, abs_diff desc

{% endtest %}
