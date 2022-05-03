{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'pending_liquidity_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.rune_tx AS rune_tx_id,
  e.pool AS pool_name,
  e.pending_type,
  e.rune_e8,
  e.asset_tx AS asset_tx_id,
  e.asset_e8,
  e.rune_addr AS rune_address,
  e.asset_addr AS asset_address,
  e.asset_chain AS asset_blockchain
FROM
  {{ ref('thorchain_dbt__pending_liquidity_events') }}
qualify(ROW_NUMBER() over(PARTITION BY ASSET, TX_COUNT, BLOCK_TIMESTAMP
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