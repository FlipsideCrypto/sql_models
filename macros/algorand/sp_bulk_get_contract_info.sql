{% macro create_algorand_sp_bulk_get_contract_info() %}
  {% set sql %}
  CREATE
  OR REPLACE PROCEDURE silver_algorand.sp_bulk_get_tx() returns variant LANGUAGE SQL AS $$
DECLARE
  RESULT variant;
row_cnt INTEGER;
BEGIN
  row_cnt:= (
    SELECT
      COUNT(1)
    FROM
      {{ ref('silver_algorand__get_tx') }}
  );
if (
    row_cnt > 0
  ) THEN RESULT:= (
    SELECT
      silver_algorand.udf_bulk_get_tx()
  );
  ELSE RESULT:= NULL;
END if;
RETURN RESULT;
END;$$ {% endset %}
{% do run_query(sql) %}
{% endmacro %}
