{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'pending_liquidity_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__pending_liquidity_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, pool, asset_tx, asset_chain, asset_addr, rune_tx, rune_addr, pending_type, block_timestamp
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
