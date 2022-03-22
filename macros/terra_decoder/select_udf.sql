{% macro select_udf() %}
    SELECT {{ terra_decode_contract() }};
{% endmacro %}