{% macro sp_bulk_get_contract_info() %}
  CREATE
  OR REPLACE PROCEDURE silver_terra.sp_bulk_get_contract_info() returns VARCHAR LANGUAGE SQL AS $$
DECLARE
  RESULT VARCHAR;
SET RESULT = (
    SELECT
      RESULT:= silver_terra.udf_bulk_get_contract_info()
  );
RETURN RESULT $$
{% endmacro %}
