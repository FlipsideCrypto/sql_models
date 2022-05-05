{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'outbound_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__outbound_events') }}
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, TX_ID, ASSET, TO_ADDRESS, FROM_ADDRESS, BLOCKCHAIN, IN_TX
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