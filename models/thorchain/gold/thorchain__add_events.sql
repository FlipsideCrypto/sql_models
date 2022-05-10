{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'add_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.rune_e8,
  e.chain AS blockchain,
  e.asset_e8,
  e.pool AS pool_name,
  e.memo,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.asset
FROM
  {{ ref('silver_thorchain__add_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp

{% if is_incremental() %}
AND e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
