{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'set_mimir_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.key,
  e.value
FROM
  {{ ref('thorchain_dbt__set_mimir_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
