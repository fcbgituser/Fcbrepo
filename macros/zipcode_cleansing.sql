{% macro zipcode_cleansing(col) -%}
(
  case
    when {{ col }} is null then null
    -- If already ZIP+4 format
    when regexp_like(cast({{ col }} as string), '^\d{5}-\d{4}$') then cast({{ col }} as string)
    -- If 9 digits with no dash → insert dash
    when regexp_like(cast({{ col }} as string), '^\d{9}$') 
      then substr(cast({{ col }} as string),1,5) || '-' || substr(cast({{ col }} as string),6,4)
    -- If exactly 5 digits
    when regexp_like(cast({{ col }} as string), '^\d{5}$') then cast({{ col }} as string)
    else cast({{ col }} as string) -- fallback: return as-is
  end
)
{%- endmacro %}
