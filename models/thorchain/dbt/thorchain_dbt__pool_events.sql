{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'pool_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_pool_events'
  ) }}
