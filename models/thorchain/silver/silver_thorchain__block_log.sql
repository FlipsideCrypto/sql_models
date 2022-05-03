{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'block_log']
) }}

SELECT
  HEIGHT,
  TIMESTAMP,
  HASH,
  AGG_STATE,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  {{ ref('thorchain_dbt__block_log') }}

qualify(ROW_NUMBER() over(PARTITION BY HEIGHT, TIMESTAMP, HASH, AGG_STATE
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}



