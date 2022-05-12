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
  {{ ref('silver_thorchain__gas_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp

{% if is_incremental() %}
  WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}