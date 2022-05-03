{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'errata_events']
) }}

SELECT
  IN_TX,
  ASSET,
  ASSET_E8,
  RUNE_E8,
  BLOCK_TIMESTAMP,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  {{ ref('thorchain_dbt__errata_events') }}

qualify(ROW_NUMBER() over(PARTITION BY IN_TX, ASSET, BLOCK_TIMESTAMP
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