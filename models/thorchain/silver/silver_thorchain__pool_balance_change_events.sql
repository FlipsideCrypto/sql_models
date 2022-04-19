{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'pool_balance_change_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.reason,
  e.asset,
  e.rune_amt AS rune_amount,
  e.asset_add,
  e.asset_amt AS asset_amount,
  e.rune_add
FROM
  {{ ref('thorchain_dbt__pool_balance_change_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
