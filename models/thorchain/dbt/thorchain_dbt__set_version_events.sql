{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'set_version_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_version_events'
  ) }}