{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'thorchain_swaps']
  )
}}

WITH swaps AS (
  SELECT * FROM {{ ref('thorchain__swap_events') }} 
  WHERE TRUE
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
)

SELECT DISTINCT
  se.block_timestamp,
  se.block_id,
  tx_id,
  blockchain,
  se.pool_name,
  from_address,
  to_address,
  from_asset,
  to_asset,
  COALESCE(from_e8 / POW(10, 8), 0) AS from_amount,
  COALESCE(to_e8 / POW(10, 8), 0) AS to_amount,
  COALESCE(to_e8_min / POW(10, 8), 0) AS min_to_amount,
  CASE
    WHEN from_asset = 'THOR.RUNE' THEN COALESCE(from_e8 * rune_usd / POW(10, 8), 0)
    ELSE COALESCE(from_e8 * asset_usd / POW(10, 8), 0)
  END AS from_amount_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(to_e8 * rune_usd / POW(10, 8), 0)
    ELSE COALESCE(to_e8 * asset_usd / POW(10, 8), 0)
  END AS to_amount_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(to_e8_min * rune_usd / POW(10, 8), 0)
    ELSE COALESCE(to_e8_min * asset_usd / POW(10, 8), 0)
  END AS to_amount_min_usd,
  swap_slip_bp,
  COALESCE(liq_fee_in_rune_e8  / POW(10, 8), 0) AS liq_fee_rune,
  COALESCE(liq_fee_in_rune_e8  / POW(10, 8) * rune_usd, 0) AS liq_fee_rune_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(liq_fee_e8 / POW(10, 8), 0)
    ELSE COALESCE(liq_fee_e8 / POW(10, 8), 0)
  END AS liq_fee_asset,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(liq_fee_e8 * rune_usd / POW(10, 8), 0)
    ELSE COALESCE(liq_fee_e8 * asset_usd / POW(10, 8), 0)
  END AS liq_fee_asset_usd
FROM swaps se
JOIN {{ ref('thorchain__prices') }} p ON se.block_id = p.block_id AND se.pool_name = p.pool_name