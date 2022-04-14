{% macro sp_bulk_get_contract_info() %}
  {% set sql %}
  CREATE
  OR REPLACE PROCEDURE silver_terra.sp_bulk_get_contract_info() returns variant LANGUAGE SQL AS $$
DECLARE
  RESULT VARCHAR;
row_cnt INTEGER;
BEGIN
  row_cnt:= (
    SELECT
      COUNT(1)
    FROM
      {{ ref('silver_terra__all_undecoded_contracts') }}
  );
if (
    row_cnt > 0
  ) THEN RESULT:= (
    SELECT
      silver_terra.udf_bulk_get_contract_info()
  );
  ELSE RESULT:= NULL;
END if;
RETURN RESULT;
END;$$ {% endset %}
{% do run_query(sql) %}
{% endmacro %}
