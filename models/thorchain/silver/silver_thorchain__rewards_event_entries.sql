{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'rewards_event_entries']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.pool AS pool_name,
  e.rune_e8
FROM
  {{ ref('thorchain_dbt__rewards_event_entries') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
