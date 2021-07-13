{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_id", "block_timestamp", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'liquidity_actions']
  )
}}

SELECT 
  se.block_timestamp, 
  se.block_id, 
  rune_tx_id AS tx_id, 
  'add_liquidity' AS lp_action, 
  se.pool_name, 
  rune_address AS from_address, 
  NULL AS to_address,
  rune_e8 / POW(10, 8) AS rune_amount,
  rune_e8 / POW(10, 8) * rune_usd AS rune_amount_usd,
  asset_e8 / POW(10, 8) AS asset_amount,
  asset_e8 / POW(10, 8) * asset_usd AS asset_amount_usd,
  stake_units,
  asset_tx_id,
  asset_address,
  asset_blockchain,
  NULL AS il_protection,
  NULL AS il_protection_usd,
  NULl AS unstake_asymmetry,
  NULL AS unstake_basis_points
FROM {{ ref('thorchain__stake_events') }} se
JOIN {{ ref('thorchain__prices') }} p ON se.block_id = p.block_id AND se.pool_name = p.pool_name

UNION

SELECT
  ue.block_timestamp, 
  ue.block_id, 
  tx_id, 
  'remove_liquidity' AS lp_action, 
  ue.pool_name, 
  from_address, 
  to_address,
  emit_rune_e8 / POW(10, 8) AS rune_amount,
  emit_rune_e8 / POW(10, 8) * rune_usd AS rune_amount_usd,
  asset_e8 / POW(10, 8) AS asset_amount,
  asset_e8 / POW(10, 8) * asset_usd AS asset_amount_usd,
  stake_units, 
  NULL AS asset_tx_id, 
  NULL AS asset_address, 
  NULL AS asset_blockchain,
  imp_loss_protection_e8 / POW(10, 8) AS il_protection,
  imp_loss_protection_e8 / POW(10, 8) * rune_usd AS il_protection_usd,
  asymmetry AS unstake_asymmetry, 
  basis_points AS unstake_basis_points
FROM {{ ref('thorchain__unstake_events') }} ue
JOIN {{ ref('thorchain__prices') }} p ON ue.block_id = p.block_id AND ue.pool_name = p.pool_name