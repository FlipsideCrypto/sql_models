{% macro call_near_views() %}
{% set sql %}

    call generate_near_views(
        SELECT 
            LISTAGG(TABLE_NAME, ',') 
        FROM "MDAO_NEAR"."INFORMATION_SCHEMA"."TABLES"
        WHERE table_schema = 'PROD'

    )

  {% endset %}

  {% do run_query(sql) %}   
{% endmacro %}