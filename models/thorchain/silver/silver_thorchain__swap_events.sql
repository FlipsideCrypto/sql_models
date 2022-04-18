{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'swap_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.chain AS blockchain,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.to_asset,
  e.from_asset,
  e.swap_slip_bp,
  e.liq_fee_in_rune_e8,
  e.liq_fee_e8,
  e.to_e8,
  e.pool AS pool_name,
  e.memo,
  e.to_e8_min,
  e.from_e8
FROM
  {{ ref('thorchain_dbt__swap_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
