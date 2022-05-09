{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'fee_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.asset,
  e.pool_deduct,
  e.asset_e8
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_fee_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
