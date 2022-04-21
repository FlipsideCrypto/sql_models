{% macro udf_bulk_get_contract_info() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver_terra.udf_bulk_get_contract_info() returns text api_integration = aws_terra_api AS {% if target.name == "prod" -%}
        'https://t8z22zn7gi.execute-api.us-east-1.amazonaws.com/prod/bulk_get_contract_info'
    {% else %}
        'https://3wcc5lj48d.execute-api.us-east-1.amazonaws.com/dev/bulk_get_contract_info'
    {%- endif %}
{% endmacro %}
