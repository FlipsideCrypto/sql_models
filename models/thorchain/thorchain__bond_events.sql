{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'bond_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.memo,
  e.asset,
  e.chain AS blockchain,
  e.bond_type,
  e.e8,
  e.asset_e8
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_bond_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
