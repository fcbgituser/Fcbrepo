{% macro zipcode_cleansing(col) -%}
(
  case
    when {{ col }} is null then null

    -- cast once to string, trim whitespace
    else
      {% set raw = "trim(cast(" ~ col ~ " as string))" %}
      {% set digits = "regexp_replace(" ~ raw ~ ", '[^0-9]', '')" %}

      case
        -- already ZIP+4 with optional surrounding whitespace
        when regexp_like({{ raw }}, '^\s*\d{5}-\d{4}\s*$') then trim({{ raw }})

        -- exactly 9 digits (no dash) -> format as ZIP+4
        when length({{ digits }}) = 9 then substr({{ digits }},1,5) || '-' || substr({{ digits }},6,4)

        -- exactly 5 digits -> return 5-digit
        when length({{ digits }}) = 5 then {{ digits }}

        -- fallback: return original trimmed string (preserves alphanumeric)
        else trim({{ raw }})
      end
  end
)
{%- endmacro %}


