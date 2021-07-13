{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["asset"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'pool_block_statistics']
  )
}}

WITH pool_depth AS (
  SELECT
    pool_name,
    rune_e8 AS rune_depth,
    asset_e8 AS asset_depth,
    rune_e8 / asset_e8 AS asset_price
  FROM (
    SELECT 
      block_id,
      pool_name,
      rune_e8,
      asset_e8,
      MAX(block_id) OVER (PARTITION BY pool_name) AS max_block_id
    FROM {{source('thorchain', 'block_pool_depths')}} 
    WHERE asset_e8 > 0
  )
  WHERE block_id = max_block_id
),
pool_status AS (
  SELECT
    pool_name,
    status
  FROM (
    SELECT 
      block_id,
      ASSET AS pool_name,
      status,
      MAX(block_id) OVER (PARTITION BY pool_name) AS max_block_id
    FROM {{source('thorchain', 'pool_events')}} 
  )
  WHERE block_id = max_block_id
),
add_liquidity_tbl AS (
  SELECT
    pool_name,
    COUNT(*) AS add_liquidity_count,
    SUM(rune_e8) AS add_rune_liquidity_volume,
    SUM(asset_e8) AS add_asset_liquidity_volume 
  FROM {{source('thorchain', 'stake_events')}} 
  GROUP BY pool_name
),
withdraw_tbl AS (
  SELECT 
    pool_name,
    COUNT(*) AS withdraw_count,
    SUM(emit_rune_e8) AS withdraw_rune_volume,
    SUM(emit_asset_e8) AS withdraw_asset_volume, 
    SUM(IMP_LOSS_PROTECTION_E8) AS impermanent_loss_protection_paid
  FROM {{source('thorchain', 'unstake_events')}}
  GROUP BY pool_name
),
swap_total_tbl AS (
  SELECT 
    pool_name,
    SUM(volume) AS swap_volume
  FROM (
    SELECT 
      pool_name, 
      CASE WHEN to_asset = 'THOR.RUNE' THEN to_e8 ELSE from_e8 END AS volume
    FROM {{source('thorchain', 'swap_events')}}
  )
  GROUP BY pool_name
),
swap_to_asset_tbl AS (
  SELECT 
    pool_name,
    SUM(LIQ_FEE_IN_RUNE_E8) AS to_asset_fees,
    SUM(from_e8) AS to_asset_volume,
    COUNT(*) AS to_asset_count,
    AVG(swap_slip_bp) AS to_asset_average_slip
  FROM(
    SELECT 
      pool_name,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 'to_rune' ELSE 'to_asset' END AS to_tune_asset,
      LIQ_FEE_IN_RUNE_E8,
      to_e8,
      from_e8,
      swap_slip_bp,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 0 ELSE LIQ_FEE_E8 END AS asset_fee
    FROM {{source('thorchain', 'swap_events')}}
  )
  GROUP BY to_tune_asset, pool_name
  HAVING to_tune_asset = 'to_asset'
),
swap_to_rune_tbl AS (
  SELECT 
    pool_name,
    SUM(LIQ_FEE_IN_RUNE_E8) AS to_rune_fees,
    SUM(to_e8) AS to_rune_volume,
    COUNT(*) AS to_rune_count,
    AVG(swap_slip_bp) AS to_rune_average_slip
  FROM(
    SELECT 
      pool_name,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 'to_rune' ELSE 'to_asset' END AS to_tune_asset,
      LIQ_FEE_IN_RUNE_E8,
      to_e8,
      from_e8,
      swap_slip_bp,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 0 ELSE LIQ_FEE_E8 END AS asset_fee
    FROM {{source('thorchain', 'swap_events')}}
  )
  GROUP BY to_tune_asset, pool_name
  HAVING to_tune_asset = 'to_rune'
),

average_slip_tbl AS (
  SELECT 
    pool_name,
    AVG(swap_slip_bp) AS average_slip
  FROM {{source('thorchain', 'swap_events')}}
  GROUP BY pool_name
),

-- // https://gitlab.com/thorchain/midgard/-/merge_requests/28/diffs
unique_swapper_tbl AS (
  SELECT 
    pool_name,
    COUNT(DISTINCT from_address) AS unique_swapper_count
  FROM {{source('thorchain', 'swap_events')}}
  GROUP BY pool_name
),

stake_amount AS (
  SELECT 
    pool_name,
    SUM(stake_units) AS units
  FROM {{source('thorchain', 'stake_events')}}
  GROUP BY pool_name
),

unstake_umc AS (
  SELECT
      from_address AS address,
      pool_name,
      SUM(stake_units) AS unstake_liquidity_units
  FROM {{source('thorchain', 'unstake_events')}}
  GROUP BY from_address, pool_name
),

stake_umc AS (
  SELECT
      rune_address AS address,
      pool_name,
      SUM(stake_units) as liquidity_units
  FROM {{source('thorchain', 'stake_events')}}
  WHERE rune_address IS NOT NULL
  GROUP BY rune_address, pool_name

  UNION ALL 

  SELECT
      asset_address AS address,
      pool_name,
      SUM(stake_units) as liquidity_units
  FROM {{source('thorchain', 'stake_events')}}
  WHERE asset_address IS NOT NULL AND rune_address IS NULL 
  GROUP BY asset_address, pool_name
),

unique_member_count AS (
  SELECT 
    pool_name,
    COUNT(DISTINCT address) AS unique_member_count
  FROM (
    SELECT  
        stake_umc.pool_name,
        stake_umc.address,
        stake_umc.liquidity_units,
        CASE WHEN unstake_umc.unstake_liquidity_units IS NOT NULL THEN unstake_umc.unstake_liquidity_units ELSE 0 END AS unstake_liquidity_units
    FROM stake_umc
    LEFT JOIN unstake_umc
    ON stake_umc.address = unstake_umc.address AND stake_umc.pool_name = unstake_umc.pool_name
  )
  WHERE liquidity_units-unstake_liquidity_units > 0
  GROUP BY pool_name
),

asset_price_usd_tbl AS (
  SELECT 
      pool_name,
      asset_usd AS asset_price_usd
  FROM (
    SELECT
      block_id,
      MAX(block_id) OVER (PARTITION BY pool_name) AS max_block_id,
      pool_name,
      asset_usd
    FROM {{ ref('thorchain__prices') }}
  )
  WHERE block_id = max_block_id
)

SELECT 
  add_asset_liquidity_volume,
  add_liquidity_count,
  (add_asset_liquidity_volume + add_rune_liquidity_volume) AS add_liquidity_volume,
  add_rune_liquidity_volume,
  pool_depth.pool_name AS asset,
  asset_depth,
  asset_price,
  asset_price_usd,
  average_slip,
  impermanent_loss_protection_paid,
-- //  poolAPY,
  rune_depth,
  status,
  (to_rune_count + to_asset_count) AS swap_count,
  swap_volume,
  to_asset_average_slip,
  to_asset_count,
  to_asset_fees,
  to_asset_volume,
  to_rune_average_slip,
  to_rune_count,
  to_rune_fees,
  to_rune_volume,
  (to_rune_fees + to_asset_fees) AS totalFees,
  unique_member_count,
  unique_swapper_count,
  units,
  withdraw_asset_volume,
  withdraw_count,
  withdraw_rune_volume,
  (withdraw_rune_volume + withdraw_asset_volume) AS withdraw_volume
FROM pool_depth

LEFT JOIN pool_status
ON pool_depth.pool_name = pool_status.pool_name

LEFT JOIN add_liquidity_tbl
ON pool_depth.pool_name = add_liquidity_tbl.pool_name

LEFT JOIN withdraw_tbl
ON pool_depth.pool_name = withdraw_tbl.pool_name

LEFT JOIN swap_total_tbl
ON pool_depth.pool_name = swap_total_tbl.pool_name

LEFT JOIN swap_to_asset_tbl
ON pool_depth.pool_name = swap_to_asset_tbl.pool_name

LEFT JOIN swap_to_rune_tbl
ON pool_depth.pool_name = swap_to_rune_tbl.pool_name

LEFT JOIN unique_swapper_tbl
ON pool_depth.pool_name = unique_swapper_tbl.pool_name

LEFT JOIN stake_amount
ON pool_depth.pool_name = stake_amount.pool_name

LEFT JOIN average_slip_tbl
ON pool_depth.pool_name = average_slip_tbl.pool_name

LEFT JOIN unique_member_count
ON pool_depth.pool_name = unique_member_count.pool_name

LEFT JOIN asset_price_usd_tbl
ON pool_depth.pool_name = asset_price_usd_tbl.pool_name
