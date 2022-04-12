{% macro udf_bulk_get_contract_info() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver_terra.udf_bulk_get_contract_info() returns text api_integration = analytics_serverless_api AS {% if target.name == "prod" -%}
        'https://knloth5ct4.execute-api.us-east-1.amazonaws.com/prod/bulk_get_contract_info'
    {% else %}
        'https://90jvvtja5m.execute-api.us-east-1.amazonaws.com/dev/bulk_get_contract_info'
    {%- endif %}
{% endmacro %}
