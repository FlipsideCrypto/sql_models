{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'pool_balance_change_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__pool_balance_change_events') }}
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, ASSET, BLOCK_TIMESTAMP
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