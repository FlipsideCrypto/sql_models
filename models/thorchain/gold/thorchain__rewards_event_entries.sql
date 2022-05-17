{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'rewards_event_entries']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.pool AS pool_name,
  e.rune_e8
FROM
  {{ ref('silver_thorchain__rewards_event_entries') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
{% if is_incremental() %}
WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}