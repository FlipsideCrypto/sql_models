{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'message_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.action,
  e.from_addr AS from_address
FROM
  {{ ref('thorchain_dbt__message_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
