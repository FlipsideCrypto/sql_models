{% macro udf_bulk_get_contract_info() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver_terra.udf_bulk_get_contract_info() returns text api_integration = analytics_serverless_api AS 'https://knloth5ct4.execute-api.us-east-1.amazonaws.com/prod/'
{% endmacro %}
