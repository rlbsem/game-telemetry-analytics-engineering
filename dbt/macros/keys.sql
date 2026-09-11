{% macro telemetry_key(fields) -%}
{# Length-prefix every field: null, empty strings, and embedded separators stay distinct. #}
md5(concat(
  {%- for field in fields %}
  coalesce(cast(length(cast({{ field }} as varchar)) as varchar), '-1'), ':',
  coalesce(cast({{ field }} as varchar), ''), ';'{% if not loop.last %},{% endif %}
  {%- endfor %}
))
{%- endmacro %}
