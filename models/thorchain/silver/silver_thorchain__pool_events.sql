{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'pool_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__pool_events') }}

qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, ASSET, STATUS, BLOCK_ID
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