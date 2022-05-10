{% macro algorand_udf_bulk_get_tx_info() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver_algorand.udf_bulk_get_tx() returns text api_integration = aws_terra_api AS {% if target.name == "prod" -%}
        'https://t8z22zn7gi.execute-api.us-east-1.amazonaws.com/prod/bulk_get_contract_info'
    {% else %}
        'https://vfc3eyv16g.execute-api.us-east-1.amazonaws.com/dev/bulk_tx'
    {%- endif %}
{% endmacro %}
