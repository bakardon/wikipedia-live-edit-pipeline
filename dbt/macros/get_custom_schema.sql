{# Use absolute schema names from `+schema:` configs (e.g. "core", "marts")
   instead of dbt's default behaviour of concatenating onto target.schema. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
