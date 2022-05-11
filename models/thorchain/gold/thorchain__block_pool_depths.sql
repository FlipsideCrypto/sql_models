{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'block_pool_depths']
) }}

SELECT
  TO_TIMESTAMP(
    d.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  d.rune_e8,
  d.asset_e8,
  d.synth_e8,
  d.pool AS pool_name
FROM
  {{ ref('silver_thorchain__block_pool_depths') }}
  d
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = d.block_timestamp

{% if is_incremental() %}
WHERE d.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}

GROUP BY
  block_timestamp,
  block_id,
  rune_e8,
  asset_e8,
  synth_e8,
  pool_name

