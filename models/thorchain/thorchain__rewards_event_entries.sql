{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'rewards_event_entries']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.pool AS pool_name,
  e.rune_e8
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_rewards_event_entries'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
