{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'new_node_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_new_node_events'
  ) }}