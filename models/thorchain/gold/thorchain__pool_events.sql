{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'pool_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset,
  e.status
FROM
  {{ ref('silver_thorchain__pool_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp

{% if is_incremental() %}
WHERE
  e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
