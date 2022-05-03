{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'add_events']
) }}

SELECT
  TX,
  CHAIN,
  FROM_ADDR,
  TO_ADDR,
  ASSET,
  ASSET_E8,
  MEMO,
  RUNE_E8,
  POOL,
  BLOCK_TIMESTAMP,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  {{ ref('thorchain_dbt__add_events') }}

qualify(ROW_NUMBER() over(PARTITION BY TX, CHAIN, FROM_ADDR, TO_ADDR, ASSET, MEMO, POOL
ORDER BY
  e.__HEVO__INGESTED_AT DESC)) = 1
