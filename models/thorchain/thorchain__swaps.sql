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
  SELECT 
    block_timestamp,
    block_id,
    tx_id,
    blockchain,
    to_address,
    from_address,
    TO_ASSET,
    FROM_ASSET,
    SWAP_SLIP_BP,
    LIQ_FEE_IN_RUNE_E8,
    LIQ_FEE_E8,    
    TO_E8,
    pool_name,
    MEMO,
    TO_E8_MIN,
    FROM_E8
  FROM {{ ref('thorchain__swap_events') }} 
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
  liq_fee_in_rune_e8  / POW(10, 8) AS liq_fee_rune,
  liq_fee_in_rune_e8  / POW(10, 8) * rune_usd AS liq_fee_rune_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN liq_fee_e8 / POW(10, 8)
    ELSE liq_fee_e8 / POW(10, 8)
  END AS liq_fee_asset,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN liq_fee_e8 * rune_usd / POW(10, 8)
    ELSE liq_fee_e8 * asset_usd / POW(10, 8)
  END AS liq_fee_asset_usd
FROM swaps se
JOIN {{ ref('thorchain__prices') }} p ON se.block_id = p.block_id AND se.pool_name = p.pool_name