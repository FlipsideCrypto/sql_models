{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'gas_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.add_asgard_addr AS add_asgard_address
FROM
  {{ ref('silver_thorchain__inactive_vault_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
