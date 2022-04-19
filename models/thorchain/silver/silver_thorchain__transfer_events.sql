{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'transfer_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset,
  e.amount_e8,
  e.from_addr AS from_address,
  e.to_addr AS to_address
FROM
  {{ ref('thorchain_dbt__transfer_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
