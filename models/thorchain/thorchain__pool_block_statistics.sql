{{ 
  config(
    materialized='table', 
    unique_key='day || asset', 
    tags=['snowflake', 'thorchain', 'thorchain_pool_block_statistics']
  )
}}

WITH pool_depth AS (
  SELECT
    day,
    pool_name,
    rune_e8 AS rune_depth,
    asset_e8 AS asset_depth,
    rune_e8 / asset_e8 AS asset_price
  FROM (
    SELECT 
      date(block_timestamp) AS day,
      block_id,
      pool_name,
      rune_e8,
      asset_e8,
      MAX(block_id) OVER (PARTITION BY pool_name, date(block_timestamp)) AS max_block_id
    FROM {{ ref("thorchain__block_pool_depths") }}
    WHERE asset_e8 > 0
  )
  WHERE block_id = max_block_id
),

pool_status AS (
  SELECT
    day,
    pool_name,
    status
  FROM (
    SELECT 
      date(block_timestamp) AS day,
      block_id,
      ASSET AS pool_name,
      status,
      MAX(block_id) OVER (PARTITION BY pool_name, date(block_timestamp)) AS max_block_id
    FROM {{ ref("thorchain__pool_events") }}
  )
  WHERE block_id = max_block_id
),

add_liquidity_tbl AS (
  SELECT
    date(block_timestamp) AS day,
    pool_name,
    COUNT(*) AS add_liquidity_count,
    SUM(rune_e8) AS add_rune_liquidity_volume,
    SUM(asset_e8) AS add_asset_liquidity_volume 
  FROM {{ ref("thorchain__stake_events") }}
  GROUP BY 1,2
),

withdraw_tbl AS (
  SELECT 
    date(block_timestamp) AS day,
    pool_name,
    COUNT(*) AS withdraw_count,
    SUM(emit_rune_e8) AS withdraw_rune_volume,
    SUM(emit_asset_e8) AS withdraw_asset_volume, 
    SUM(IMP_LOSS_PROTECTION_E8) AS impermanent_loss_protection_paid
  FROM {{ ref("thorchain__unstake_events") }}
  GROUP BY 1,2
),

swap_total_tbl AS (
  SELECT 
    day,
    pool_name,
    SUM(volume) AS swap_volume
  FROM (
    SELECT 
      date(block_timestamp) AS day,
      pool_name, 
      CASE WHEN to_asset = 'THOR.RUNE' THEN to_e8 ELSE from_e8 END AS volume
    FROM {{ ref("thorchain__swap_events") }}
  )
  GROUP BY 1,2
),

swap_to_asset_tbl AS (
  SELECT 
    day,
    pool_name,
    SUM(LIQ_FEE_IN_RUNE_E8) AS to_asset_fees,
    SUM(from_e8) AS to_asset_volume,
    COUNT(*) AS to_asset_count,
    AVG(swap_slip_bp) AS to_asset_average_slip
  FROM(
    SELECT 
      date(block_timestamp) AS day,
      pool_name,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 'to_rune' ELSE 'to_asset' END AS to_tune_asset,
      LIQ_FEE_IN_RUNE_E8,
      to_e8,
      from_e8,
      swap_slip_bp,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 0 ELSE LIQ_FEE_E8 END AS asset_fee
    FROM {{ ref("thorchain__swap_events") }}
  )
  GROUP BY to_tune_asset, pool_name, day
  HAVING to_tune_asset = 'to_asset'
),
swap_to_rune_tbl AS (
  SELECT 
    day,
    pool_name,
    SUM(LIQ_FEE_IN_RUNE_E8) AS to_rune_fees,
    SUM(to_e8) AS to_rune_volume,
    COUNT(*) AS to_rune_count,
    AVG(swap_slip_bp) AS to_rune_average_slip
  FROM(
    SELECT 
      date(block_timestamp) AS day,
      pool_name,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 'to_rune' ELSE 'to_asset' END AS to_tune_asset,
      LIQ_FEE_IN_RUNE_E8,
      to_e8,
      from_e8,
      swap_slip_bp,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 0 ELSE LIQ_FEE_E8 END AS asset_fee
    FROM {{ ref("thorchain__swap_events") }}
  )
  GROUP BY to_tune_asset, pool_name, day
  HAVING to_tune_asset = 'to_rune'
),

average_slip_tbl AS (
  SELECT 
    date(block_timestamp) AS day,
    pool_name,
    AVG(swap_slip_bp) AS average_slip
  FROM {{ ref("thorchain__swap_events") }}
  GROUP BY pool_name, day
),

-- // https://gitlab.com/thorchain/midgard/-/merge_requests/28/diffs
unique_swapper_tbl AS (
  SELECT 
    date(block_timestamp) AS day,
    pool_name,
    COUNT(DISTINCT from_address) AS unique_swapper_count
  FROM {{ ref("thorchain__swap_events") }}
  GROUP BY pool_name, day
),

stake_amount AS (
  SELECT 
    date(block_timestamp) AS day,
    pool_name,
    SUM(stake_units) AS units
  FROM {{ ref("thorchain__stake_events") }}
  GROUP BY pool_name, day
),

unstake_umc AS (
  SELECT
      date(block_timestamp) AS day,
      from_address AS address,
      pool_name,
      SUM(stake_units) AS unstake_liquidity_units
  FROM {{ ref("thorchain__unstake_events") }}
  GROUP BY from_address, pool_name, day
),

stake_umc AS (
  SELECT
      date(block_timestamp) AS day,
      rune_address AS address,
      pool_name,
      SUM(stake_units) as liquidity_units
  FROM {{ ref("thorchain__stake_events") }}
  WHERE rune_address IS NOT NULL
  GROUP BY rune_address, pool_name, day

  UNION ALL 

  SELECT
      date(block_timestamp) AS day,
      asset_address AS address,
      pool_name,
      SUM(stake_units) as liquidity_units
  FROM {{ ref("thorchain__stake_events") }}
  WHERE 
    asset_address IS NOT NULL AND 
    rune_address IS NULL 
  GROUP BY asset_address, pool_name, day
),

unique_member_count AS (
  SELECT 
    day,
    pool_name,
    COUNT(DISTINCT address) AS unique_member_count
  FROM (
    SELECT  
        stake_umc.day,
        stake_umc.pool_name,
        stake_umc.address,
        stake_umc.liquidity_units,
        CASE WHEN unstake_umc.unstake_liquidity_units IS NOT NULL THEN unstake_umc.unstake_liquidity_units ELSE 0 END AS unstake_liquidity_units
    FROM stake_umc
    LEFT JOIN unstake_umc
    ON stake_umc.address = unstake_umc.address AND stake_umc.pool_name = unstake_umc.pool_name
  )
  WHERE liquidity_units-unstake_liquidity_units > 0
  GROUP BY pool_name, day
),

asset_price_usd_tbl AS (
  SELECT 
      day,
      pool_name,
      asset_usd AS asset_price_usd
  FROM (
    SELECT
      date(block_timestamp) AS day,
      block_id,
      MAX(block_id) OVER (PARTITION BY pool_name, date(block_timestamp)) AS max_block_id,
      pool_name,
      asset_usd
    FROM {{ ref("thorchain__prices") }}
  )
  WHERE block_id = max_block_id
)

SELECT 
  pool_depth.day AS day,
  COALESCE(add_asset_liquidity_volume, 0) AS add_asset_liquidity_volume,
  COALESCE(add_liquidity_count, 0) AS add_liquidity_count,
  (COALESCE(add_asset_liquidity_volume, 0) + COALESCE(add_rune_liquidity_volume, 0)) AS add_liquidity_volume,
  COALESCE(add_rune_liquidity_volume, 0) AS add_rune_liquidity_volume,
  pool_depth.pool_name AS asset,
  asset_depth,
  COALESCE(asset_price, 0) AS asset_price,
  COALESCE(asset_price_usd, 0) AS asset_price_usd,
  COALESCE(average_slip, 0) AS average_slip,
  COALESCE(impermanent_loss_protection_paid, 0) AS impermanent_loss_protection_paid,
  COALESCE(rune_depth, 0) AS rune_depth,
  COALESCE(status, 'no status') AS status,
  COALESCE((to_rune_count + to_asset_count), 0) AS swap_count,
  COALESCE(swap_volume, 0) AS swap_volume,
  COALESCE(to_asset_average_slip, 0) AS to_asset_average_slip,
  COALESCE(to_asset_count, 0) AS to_asset_count,
  COALESCE(to_asset_fees, 0) AS to_asset_fees,
  COALESCE(to_asset_volume, 0) AS to_asset_volume,
  COALESCE(to_rune_average_slip, 0) AS to_rune_average_slip,
  COALESCE(to_rune_count, 0) AS to_rune_count,
  COALESCE(to_rune_fees, 0) AS to_rune_fees,
  COALESCE(to_rune_volume, 0) AS to_rune_volume,
  (COALESCE(to_rune_fees, 0) + COALESCE(to_asset_fees, 0)) AS totalFees,
  COALESCE(unique_member_count, 0) AS unique_member_count,
  COALESCE(unique_swapper_count, 0) AS unique_swapper_count,
  COALESCE(units, 0) AS units,
  COALESCE(withdraw_asset_volume, 0) AS withdraw_asset_volume,
  COALESCE(withdraw_count, 0) AS withdraw_count,
  COALESCE(withdraw_rune_volume, 0) AS withdraw_rune_volume,
  (COALESCE(withdraw_rune_volume, 0) + COALESCE(withdraw_asset_volume, 0)) AS withdraw_volume
FROM pool_depth

LEFT JOIN pool_status
ON pool_depth.pool_name = pool_status.pool_name AND pool_depth.day = pool_status.day

LEFT JOIN add_liquidity_tbl
ON pool_depth.pool_name = add_liquidity_tbl.pool_name AND pool_depth.day = add_liquidity_tbl.day

LEFT JOIN withdraw_tbl
ON pool_depth.pool_name = withdraw_tbl.pool_name AND pool_depth.day = withdraw_tbl.day

LEFT JOIN swap_total_tbl
ON pool_depth.pool_name = swap_total_tbl.pool_name AND pool_depth.day = swap_total_tbl.day

LEFT JOIN swap_to_asset_tbl
ON pool_depth.pool_name = swap_to_asset_tbl.pool_name AND pool_depth.day = swap_to_asset_tbl.day

LEFT JOIN swap_to_rune_tbl
ON pool_depth.pool_name = swap_to_rune_tbl.pool_name AND pool_depth.day = swap_to_rune_tbl.day

LEFT JOIN unique_swapper_tbl
ON pool_depth.pool_name = unique_swapper_tbl.pool_name AND pool_depth.day = unique_swapper_tbl.day

LEFT JOIN stake_amount
ON pool_depth.pool_name = stake_amount.pool_name AND pool_depth.day = stake_amount.day

LEFT JOIN average_slip_tbl
ON pool_depth.pool_name = average_slip_tbl.pool_name AND pool_depth.day = average_slip_tbl.day

LEFT JOIN unique_member_count
ON pool_depth.pool_name = unique_member_count.pool_name AND pool_depth.day = unique_member_count.day

LEFT JOIN asset_price_usd_tbl
ON pool_depth.pool_name = asset_price_usd_tbl.pool_name AND pool_depth.day = asset_price_usd_tbl.day