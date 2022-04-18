{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'update_node_account_status_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.node_addr AS node_address,
  e."CURRENT" AS current_status,
  e.former AS former_status
FROM
  {{ ref('thorchain_dbt__update_node_account_status_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
