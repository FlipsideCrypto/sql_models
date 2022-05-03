{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'asgard_fund_yggdrasil_events']
) }}

SELECT
  block_timestamp,
  height AS block_id,
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

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id, asset, vault_key
ORDER BY
  e.__HEVO__INGESTED_AT DESC)) = 1