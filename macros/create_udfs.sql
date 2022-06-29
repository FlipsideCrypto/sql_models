{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_asset_metadata() }};
    {% endset %}
    {% do run_query(sql) %}
    {% set sql %}
    {{ udf_bulk_get_balances() }};
    {% endset %}
    {% do run_query(sql) %}
    {% set sql %}
    {{ udf_bulk_get_validator_metadata() }};
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
