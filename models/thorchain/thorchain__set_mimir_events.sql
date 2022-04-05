{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'set_mimir_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.key,
  e.value
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_mimir_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
