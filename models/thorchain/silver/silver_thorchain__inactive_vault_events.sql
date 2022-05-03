{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'gas_events']
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
  {{ ref('thorchain_dbt__inactive_vault_events') }}

qualify(ROW_NUMBER() over(PARTITION BY ADD_ASGARD_ADDR, BLOCK_TIMESTAMP
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}