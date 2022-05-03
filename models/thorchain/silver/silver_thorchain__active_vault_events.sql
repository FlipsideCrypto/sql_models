{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'active_vault_events']
) }}

SELECT
  ADD_ASGARD_ADDR,
  BLOCK_TIMESTAMP,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  {{ ref('thorchain_dbt__active_vault_events') }}

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id, pool_name, to_address, from_address, asset
ORDER BY
  e.__HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE __HEVO__LOADED_AT >= (
  SELECT
    MAX(__HEVO__LOADED_AT)
  FROM
    {{ this }}
)
{% endif %}