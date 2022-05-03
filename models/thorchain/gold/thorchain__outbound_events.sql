{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'outbound_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.asset,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.memo,
  e.asset_e8,
  e.chain AS blockchain,
  e.in_tx
FROM
  {{ ref('silver_thorchain__outbound_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
