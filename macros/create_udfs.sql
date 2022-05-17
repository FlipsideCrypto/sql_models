{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_contract_info() }};
{# Add crate udf macros here #}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
