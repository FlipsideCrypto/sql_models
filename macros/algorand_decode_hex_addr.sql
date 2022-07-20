{% macro create_algorand_decode_hex_addr() %}
    {% set sql %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION algorand_decode_hex_addr(
        addr STRING
    ) returns STRING api_integration = analytics_serverless_api max_batch_rows = 300 AS 'https://yww9kipdth.execute-api.us-east-1.amazonaws.com/api/algorand/decode-hex-address';
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
