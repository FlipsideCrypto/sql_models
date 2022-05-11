{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'message_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.action,
  e.from_addr AS from_address
FROM
  {{ ref('silver_thorchain__message_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
