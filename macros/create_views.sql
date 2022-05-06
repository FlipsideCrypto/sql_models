{% macro create_views() %}
    CREATE OR REPLACE SCHEMA FLIPSIDE_DEV_DB.MDAO_HARMONY_DYNAMIC; 

    SET query_text = (
        SELECT 
                LISTAGG(TABLE_NAME, ',') 
        FROM "MDAO_HARMONY"."INFORMATION_SCHEMA"."TABLES"
        WHERE table_schema = 'PROD'); 

    CALL generate_views_code($query_text); 
{% endmacro %}