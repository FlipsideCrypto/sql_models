{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'active_vault_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.add_asgard_addr
FROM
  {{ ref('thorchain_dbt__active_vault_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
