{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'errata_events']
) }}

SELECT 
  *
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