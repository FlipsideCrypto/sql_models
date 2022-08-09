{% macro create_udfs() %}
    {% set sql %}
    {{ create_algorand_udf_bulk_get_tx_info() }};
{{ create_algorand_udf_bulk_get_tx_info() }};
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
