{% macro call_views() %}
{% set sql %}

    call generate_harmony_views(
        SELECT 
            LISTAGG(TABLE_NAME, ',') 
        FROM "MDAO_HARMONY"."INFORMATION_SCHEMA"."TABLES"
        WHERE table_schema = 'PROD'

    )

  {% endset %}

  {% do run_query(sql) %}   
{% endmacro %}