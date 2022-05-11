{% macro create_algorand_udf_bulk_get_tx_info() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver_algorand.udf_bulk_get_tx() returns text api_integration = aws_algorand_api AS {% if target.name == "prod" -%}
        'https://koklizm6jc.execute-api.us-east-1.amazonaws.com/prod/bulk_get_tx_info'
    {% else %}
        'https://vfc3eyv16g.execute-api.us-east-1.amazonaws.com/dev/bulk_get_tx_info'
    {%- endif %}
{% endmacro %}
