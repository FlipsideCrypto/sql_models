{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'pending_liquidity_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__pending_liquidity_events') }}
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, RUNE_TX_ID, BLOCK_TIMESTAMP, POOL_NAME, PENDING_TYPE, ASSET_TX_ID, RUNE_ADDRESS, ASSET_ADDRESS
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