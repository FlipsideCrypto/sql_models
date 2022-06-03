{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', tx_id, block_id, to_asset, from_asset, block_timestamp, to_address, from_address, pool_name, memo)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain', 'swap_events']
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
  {{ ref('silver_thorchain__swap_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp

{% if is_incremental() %}
WHERE
  e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
