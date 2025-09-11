{% macro phone_number_cleansing(col) -%}
(
  case
    when {{ col }} is null then null

    else
      {% set raw = "trim(cast(" ~ col ~ " as string))" %}
      {% set digits = "regexp_replace(" ~ raw ~ ", '[^0-9]', '')" %}

      case
        -- If exactly 10 digits → format as XXX-XXX-XXXX
        when length({{ digits }}) = 10
          then substr({{ digits }},1,3) || '-' ||
               substr({{ digits }},4,3) || '-' ||
               substr({{ digits }},7,4)

        -- If 11 digits starting with 1 → drop leading 1 and format
        when length({{ digits }}) = 11 and substr({{ digits }},1,1) = '1'
          then substr({{ digits }},2,3) || '-' ||
               substr({{ digits }},5,3) || '-' ||
               substr({{ digits }},8,4)

        -- Already in XXX-XXX-XXXX format → keep as is
        when regexp_like({{ raw }}, '^\s*\d{3}-\d{3}-\d{4}\s*$')
          then trim({{ raw }})

        -- Otherwise return original (so alphanumeric or invalid won’t error)
        else trim({{ raw }})
      end
  end
)
{%- endmacro %}
