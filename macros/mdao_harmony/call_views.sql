CREATE OR REPLACE SCHEMA FLIPSIDE_DEV_DB.MDAO_HARMONY; 
USE DATABASE FLIPSIDE_DEV_DB; 
USE SCHEMA MDAO_HARMONY; 

SET table_names = (
   SELECT 
        LISTAGG(TABLE_NAME, ',') 
   FROM ref({'mdao_harmony__information_schema__tables'})
   WHERE table_schema = 'PROD'); 

{{ create_views($table_names) }} 