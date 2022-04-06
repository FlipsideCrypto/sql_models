{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'message_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.action,
  e.from_addr AS from_address
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_message_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
