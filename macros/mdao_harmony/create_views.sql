{% macro create_views(table_names) %}

CREATE OR REPLACE PROCEDURE CREATE_VIEWS(table_names STRING)
  returns STRING
  language javascript
  as     
  $$
  
    var table_names_ARR = table_names.split(','); 
    
    var table_names_LENGTH = table_names_ARR.length; 
    
    for (i=0; i<table_names_LENGTH; i++){
  
      var get_tables_name_stmt = "SELECT Table_Name FROM MDAO_HARMONY.INFORMATION_SCHEMA.TABLES \
              WHERE table_schema = 'PROD' AND TABLE_NAME LIKE UPPER('"+ table_names_ARR[i] + "%');"

      var get_tables_name_stmt = snowflake.createStatement({sqlText:get_tables_name_stmt });

      var tables = get_tables_name_stmt.execute();

      var create_statement = "CREATE OR REPLACE VIEW FLIPSIDE_DEV_DB.MDAO_HARMONY." + table_names_ARR[i] +" AS \n";

      create_statement += "SELECT * FROM MDAO_HARMONY.PROD." + table_names_ARR[i];  

      var create_view_statement = snowflake.createStatement( {sqlText: create_statement} );

      create_view_statement.execute();
      
    }

   return create_view_statement.getSqlText();
    
  $$

  {% endmacro %}