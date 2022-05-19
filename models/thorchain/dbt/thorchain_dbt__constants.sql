{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'constants']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_constants'
  ) }}
