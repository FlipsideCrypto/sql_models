{% macro terra_decode_contract() %}
 CREATE OR REPLACE EXTERNAL FUNCTION {{ target.schema }}.terra_decode_contract(token_contract varchar)
    RETURNS text
    api_integration = analytics_serverless_api
    AS 'https://a8i70ujg06.execute-api.us-east-1.amazonaws.com/prod/bulk_get_contract_info'
{% endmacro %}