{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'pending_liquidity_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__pending_liquidity_events') }}
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_TIMESTAMP, POOL, PENDING_TYPE, ASSET_TX, RUNE_ADDR, ASSET_ADDR
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