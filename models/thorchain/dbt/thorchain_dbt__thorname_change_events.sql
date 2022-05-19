{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'thorname_change_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_thorname_change_events'
  ) }}