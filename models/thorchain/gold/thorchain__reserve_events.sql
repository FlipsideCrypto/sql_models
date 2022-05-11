{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'reserve_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.chain AS blockchain,
  e.addr AS address,
  e.e8,
  e.asset_e8,
  e.from_addr AS from_address,
  e.to_addr AS to_address,
  e.memo,
  e.asset
FROM
  {{ ref('silver_thorchain__reserve_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
{% if is_incremental() %}
WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}