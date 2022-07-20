{% macro create_algorand_decode_b64_addr() %}
    {% set sql %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION algorand_decode_b64_addr(
        addr STRING
    ) returns STRING api_integration = analytics_serverless_api max_batch_rows = 300 AS 'https://yww9kipdth.execute-api.us-east-1.amazonaws.com/api/algorand/decode-b64-address';
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
