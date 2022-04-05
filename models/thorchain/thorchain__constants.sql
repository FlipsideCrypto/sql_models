{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'constants']
) }}

SELECT
  C.key,
  C.value
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_constants'
  ) }} C
