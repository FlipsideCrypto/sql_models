{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'constants']
) }}

SELECT
  C.key,
  C.value
FROM
  {{ ref('thorchain_dbt__constants') }} C
