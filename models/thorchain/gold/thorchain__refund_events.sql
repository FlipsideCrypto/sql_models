{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'refund_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.asset_e8,
  e.memo,
  e.reason,
  e.asset_2nd_e8,
  e.code,
  e.chain AS blockchain,
  e.asset,
  e.asset_2nd,
  e.to_addr AS to_address,
  e.from_addr AS from_address
FROM
  {{ ref('silver_thorchain__refund_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp

{% if is_incremental() %}
WHERE
  e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
