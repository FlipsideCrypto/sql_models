{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'add_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.rune_e8,
  e.chain AS blockchain,
  e.asset_e8,
  e.pool AS pool_name,
  e.memo,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.asset
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_add_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
