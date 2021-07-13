{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_id", "block_timestamp", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'swaps']
  )
}}


SELECT
  se.block_timestamp,
  se.block_id,
  tx_id,
  blockchain,
  se.pool_name,
  from_address,
  to_address,
  from_asset,
  to_asset,
  from_e8 / POW(10, 8) AS from_amount,
  to_e8 / POW(10, 8) AS to_amount,
  to_e8_min / POW(10, 8) AS min_to_amount,
  CASE
    WHEN from_asset = 'THOR.RUNE' THEN from_e8 * rune_usd / POW(10, 8)
    ELSE from_e8 * asset_usd / POW(10, 8)
  END AS from_amount_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN to_e8 * rune_usd / POW(10, 8)
    ELSE to_e8 * asset_usd / POW(10, 8)
  END AS to_amount_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN to_e8_min * rune_usd / POW(10, 8)
    ELSE to_e8_min * asset_usd / POW(10, 8)
  END AS to_amount_min_usd,
  swap_slip_bp,
  liq_fee_in_rune_e8 AS liq_fee_rune,
  liq_fee_in_rune_e8 * rune_usd AS liq_fee_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN liq_fee_e8 * rune_usd / POW(10, 8)
    ELSE liq_fee_e8 * asset_usd / POW(10, 8)
  END AS liq_fee_asset
FROM {{ ref('thorchain__swap_events') }} se
JOIN {{ ref('thorchain__prices') }} p ON se.block_id = p.block_id AND se.pool_name = p.pool_name