{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'asgard_fund_yggdrasil_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset,
  e.tx AS tx_id,
  e.vault_key,
  e.asset_e8
FROM
  {{ ref('thorchain_dbt__asgard_fund_yggdrasil_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
