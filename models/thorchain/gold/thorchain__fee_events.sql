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
  {{ ref('silver_thorchain__fee_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp

{% if is_incremental() %}
  WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}