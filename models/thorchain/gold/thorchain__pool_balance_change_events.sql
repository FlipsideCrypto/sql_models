{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'pool_balance_change_events']
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
  {{ ref('silver_thorchain__pool_balance_change_events') }}
  e
  INNER JOIN {{ ref('siler_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
