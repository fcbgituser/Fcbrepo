{% macro zipcode_cleansing(col) -%}
(
  case
    when {{ col }} is null then null

    -- normalize digits only once for length checks / formatting
    else
      {% set digits = "regexp_replace(cast(" ~ col ~ " as string), '[^0-9]', '')" %}
      case
        when length({{ digits }}) = 9 then substr({{ digits }}, 1, 5) || '-' || substr({{ digits }}, 6, 4)
        when length({{ digits }}) = 5 then {{ digits }}
        when regexp_like(cast({{ col }} as string), '^\s*\d{5}-\d{4}\s*$') then trim(cast({{ col }} as string))
        else trim(cast({{ col }} as string))  -- fallback: return original (change to NULL if you prefer)
      end
  end
)
{%- endmacro %}

