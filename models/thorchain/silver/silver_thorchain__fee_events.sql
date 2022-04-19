{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'fee_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.asset,
  e.pool_deduct,
  e.asset_e8
FROM
  {{ ref('thorchain_dbt__fee_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
