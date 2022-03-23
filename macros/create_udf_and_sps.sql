{% macro create_udfs() %}
    {{ terra_decode_contract() }};
{% endmacro %}

{% macro create_sps() %}
    {{ terra_decode_sp() }};
{% endmacro %}
