{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {% set node_name = node.name %}
    {% set split_name = node_name.split('__') %}
    {{ split_name[0] | trim }}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {% set node_name = node.name %}
    {% set split_name = node_name.split('__') %}
    {{ split_name[1] | trim }}
{%- endmacro %}
