{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_timestamp", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'pool_block_fees']
  )
}}

WITH all_block_id AS (
  SELECT DISTINCT
      date(block_timestamp) AS day,
      pool_name
  FROM {{source('thorchain', 'block_pool_depths')}} 
  ORDER BY 1 DESC
),

total_pool_rewards_tbl AS (
  SELECT
      date(block_timestamp) AS day,
      pool_name,
      SUM(rune_e8) AS rewards
  FROM {{source('thorchain', 'rewards_event_entries')}} 
  GROUP BY 1,2
  ORDER BY 1 DESC
),

total_liquidity_fees_rune_tbl AS (
  SELECT 
      date(block_timestamp) AS day,
      pool_name,
      SUM(LIQ_FEE_IN_RUNE_E8) AS total_liquidity_fees_rune
  FROM {{source('thorchain', 'swap_events')}} 
  GROUP BY 1,2
  ORDER BY 1 DESC
),

liquidity_fees_asset_tbl AS (
  SELECT 
    date(block_timestamp) AS day,
    pool_name,
    SUM(asset_fee) AS assetLiquidityFees
  FROM (
    SELECT
      block_timestamp,
      pool_name,
      CASE WHEN to_asset = 'THOR.RUNE' THEN 0 ELSE LIQ_FEE_E8 END AS asset_fee
    FROM {{source('thorchain', 'swap_events')}} 
  )
  GROUP BY 1,2
  ORDER BY 1 DESC
),

liquidity_fees_rune_tbl AS (
  SELECT 
    date(block_timestamp) AS day,
    pool_name,
    SUM(asset_fee) AS runeLiquidityFees
  FROM (
    SELECT
      block_timestamp,
      pool_name,
      CASE WHEN to_asset <> 'THOR.RUNE' THEN 0 ELSE LIQ_FEE_E8 END AS asset_fee
    FROM {{source('thorchain', 'swap_events')}} 
  )
  GROUP BY 1,2
  ORDER BY 1 DESC
)

SELECT
    all_block_id.day,
    all_block_id.pool_name,
    rewards,
    total_liquidity_fees_rune,
    assetLiquidityFees AS asset_liquidity_fees,
    runeLiquidityFees AS rune_liquidity_fees,
    (total_liquidity_fees_rune + rewards) AS earnings
FROM all_block_id
LEFT JOIN total_pool_rewards_tbl
ON all_block_id.day = total_pool_rewards_tbl.day AND all_block_id.pool_name = total_pool_rewards_tbl.pool_name
LEFT JOIN total_liquidity_fees_rune_tbl
ON all_block_id.day = total_liquidity_fees_rune_tbl.day AND all_block_id.pool_name = total_liquidity_fees_rune_tbl.pool_name
LEFT JOIN liquidity_fees_asset_tbl
ON all_block_id.day = liquidity_fees_asset_tbl.day AND all_block_id.pool_name = liquidity_fees_asset_tbl.pool_name
LEFT JOIN liquidity_fees_rune_tbl
ON all_block_id.day = liquidity_fees_rune_tbl.day AND all_block_id.pool_name = liquidity_fees_rune_tbl.pool_name
ORDER BY 1 DESC