{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'block_pool_depths']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__block_pool_depths') }}

qualify(ROW_NUMBER() over(PARTITION BY block_id, block_timestamp, pool_name
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