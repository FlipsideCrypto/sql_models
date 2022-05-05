{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'unstake_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.chain AS blockchain,
  e.pool AS pool_name,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.asset,
  e.emit_rune_e8,
  e.asymmetry,
  e.asset_e8,
  e.stake_units,
  e.memo,
  e.emit_asset_e8,
  e.imp_loss_protection_e8,
  e.basis_points,
  e._EMIT_ASSET_IN_RUNE_E8
FROM
  {{ ref(
    'thorchain_dbt__unstake_events'
  ) }}
  e
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, TX_ID, BLOCK_TIMESTAMP, POOL_NAME, ASSET, FROM_ADDRESS, TO_ADDRESS
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