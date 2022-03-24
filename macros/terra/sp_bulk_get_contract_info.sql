{% macro sp_bulk_get_contract_info() %}
  CREATE
  OR REPLACE PROCEDURE silver_terra.sp_bulk_get_contract_info() returns VARCHAR LANGUAGE SQL AS $$
DECLARE
  RESULT VARCHAR;
BEGIN
  RESULT:= (
    SELECT
      silver_terra.terra_decode_contract()
  );
RETURN RESULT;
END;$$
{% endmacro %}
