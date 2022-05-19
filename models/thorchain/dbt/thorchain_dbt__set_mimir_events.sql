{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'set_mimir_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_mimir_events'
  ) }}
