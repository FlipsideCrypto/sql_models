{% macro terra_decode_sp() %}
CREATE OR REPLACE PROCEDURE terra_decode_sp()
  RETURNS VARCHAR  
  LANGUAGE JAVASCRIPT  
AS  
$$  
var cmd = "SELECT terra_decode_contract()"
var sql = snowflake.createStatement({sqlText: cmd});
var result = sql.execute();
return 'success';
$$;
{% endmacro %}