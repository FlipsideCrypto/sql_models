{% macro create_udfs() %}
    {{ udf_bulk_get_contract_info() }};
{% endmacro %}
