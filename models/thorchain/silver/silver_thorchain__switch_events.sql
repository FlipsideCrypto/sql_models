{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'switch_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.burn_asset,
  e.burn_e8,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.tx
FROM
  {{ ref('thorchain_dbt__switch_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
GROUP BY
  block_timestamp,
  block_id,
  burn_asset,
  burn_e8,
  to_address,
  tx,
  from_address
