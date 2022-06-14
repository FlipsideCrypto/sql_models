{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_contract_info() }};
{{ create_algorand_udf_bulk_get_tx_info() }};
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
