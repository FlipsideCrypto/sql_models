{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'outbound_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__outbound_events') }}
qualify(ROW_NUMBER() over(PARTITION BY TX, CHAIN, FROM_ADDR, TO_ADDR, ASSET, ASSET_E8, MEMO, IN_TX, BLOCK_TIMESTAMP
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