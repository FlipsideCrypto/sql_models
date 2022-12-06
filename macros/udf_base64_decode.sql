{% macro create_udf_base64_decode() %}
    {% set sql %}
create or replace function UDF_base64_decode(input string)
returns string
language python
runtime_version = '3.8'
HANDLER = 'base64_decode'
as
$$
def base64_decode(input):
    import base64
    result = base64.b64decode(input)
    return result
$$;
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
