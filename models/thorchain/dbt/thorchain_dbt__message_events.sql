{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'message_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_message_events'
  ) }}