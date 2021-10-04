{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='event_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'thorchain_liquidity_actions']
  )
}}

WITH stakes AS (
  SELECT 
    block_timestamp,
    block_id,
    event_id,
    rune_tx_id,
    pool_name,
    RUNE_E8,
    rune_address,
    STAKE_UNITS,
    asset_tx_id,
    ASSET_E8,
    asset_address,
    asset_blockchain
  FROM {{ ref('thorchain__stake_events') }}
  WHERE TRUE
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),
unstakes AS (
  SELECT 
    block_timestamp,
    block_id,
    event_id,
    tx_id,
    blockchain,
    pool_name,
    to_address,
    from_address,
    ASSET,
    EMIT_RUNE_E8,
    ASYMMETRY,
    ASSET_E8,
    STAKE_UNITS,
    MEMO,
    EMIT_ASSET_E8,
    IMP_LOSS_PROTECTION_E8,
    BASIS_POINTS
  FROM {{ ref('thorchain__unstake_events') }}
  WHERE TRUE
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
)

SELECT 
  se.block_timestamp, 
  se.block_id, 
  se.event_id, 
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
FROM stakes se

JOIN {{ ref('thorchain__prices') }} p 
ON se.block_id = p.block_id AND se.pool_name = p.pool_name

UNION

SELECT
  ue.block_timestamp, 
  ue.block_id, 
  ue.event_id,
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
FROM unstakes ue

JOIN {{ ref('thorchain__prices') }} p 
ON ue.block_id = p.block_id AND ue.pool_name = p.pool_name