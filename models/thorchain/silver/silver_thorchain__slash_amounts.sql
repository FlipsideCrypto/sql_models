{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'slash_amounts']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset_e8,
  e.pool AS pool_name,
  e.asset
FROM
  {{ ref('thorchain_dbt__slash_amounts') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
