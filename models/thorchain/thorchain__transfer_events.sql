{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'transfer_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset,
  e.amount_e8,
  e.from_addr AS from_address,
  e.to_addr AS to_address
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_transfer_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
