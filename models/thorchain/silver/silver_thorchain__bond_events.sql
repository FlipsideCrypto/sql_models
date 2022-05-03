{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'bond_events']
) }}

SELECT 
  TX,
  CHAIN,
  FROM_ADDR,
  TO_ADDR,
  ASSET,
  ASSET_E8,
  MEMO,
  BOND_TYPE,
  E8,
  BLOCK_TIMESTAMP,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  {{ ref('thorchain_dbt__bond_events') }}

qualify(ROW_NUMBER() over(PARTITION BY TX, CHAIN, FROM_ADDR, TO_ADDR, ASSET, BOND_TYPE
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1