{% macro sp_bulk_get_contract_info() %}
  CREATE
  OR REPLACE PROCEDURE silver_terra.sp_bulk_get_contract_info() returns variant LANGUAGE SQL AS $$
DECLARE
  RESULT VARCHAR;
BEGIN
  RESULT:= (
    SELECT
      silver_terra.udf_bulk_get_contract_info()
  );
RETURN RESULT;
END;$$
{% endmacro %}
