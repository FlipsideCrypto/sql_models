{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'gas_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.rune_e8,
  e.tx_count,
  e.asset_e8,
  e.asset
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_gas_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
