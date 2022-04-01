{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'pending_liquidity_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.rune_tx AS rune_tx_id,
  e.pool AS pool_name,
  e.pending_type,
  e.rune_e8,
  e.asset_tx AS asset_tx_id,
  e.asset_e8,
  e.rune_addr AS rune_address,
  e.asset_addr AS asset_address,
  e.asset_chain AS asset_blockchain
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_pending_liquidity_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
