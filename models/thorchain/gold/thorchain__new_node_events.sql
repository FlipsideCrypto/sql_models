{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'new_node_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.node_addr AS node_address
FROM
  {{ ref('silver_thorchain__new_node_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
