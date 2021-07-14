{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["day", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'daily_pool_stats']
  )
}}

WITH max_daily_block AS (
  SELECT 
    max(block_id) AS block_id,
    date_trunc('day', block_timestamp) AS day
  FROM {{ ref("thorchain__prices") }}
  GROUP BY day
),

daily_rune_price AS (
  SELECT
    p.block_id,
    pool_name,
    day,
    rune_usd,
    asset_usd
  FROM {{ ref("thorchain__prices") }} p
  JOIN max_daily_block mdb 
  WHERE p.block_id = mdb.block_id
),

pool_fees AS (
  SELECT 
    pbf.day,
    pbf.pool_name,
    rewards AS system_rewards,
    rewards * rune_usd AS system_rewards_usd,
    asset_liquidity_fees,
    asset_liquidity_fees * asset_usd AS asset_liqudity_fees_usd,
    rune_liquidity_fees,
    rune_liquidity_fees * rune_usd AS rune_liquidity_fees_usd
  FROM {{ ref("thorchain__pool_block_fees") }} pbf
  JOIN daily_rune_price drp ON pbf.day = drp.day AND pbf.pool_name = drp.pool_name
)

SELECT
  pbs.day,
  --pbs."status",
  pf.pool_name,
  system_rewards,
  system_rewards_usd,
  asset_liquidity_fees,
  asset_liqudity_fees_usd,
  rune_liquidity_fees,
  rune_liquidity_fees_usd,
  asset_depth / POW(10, 8) AS asset_liquidity,
  asset_price,
  asset_price_usd,
  rune_depth / POW(10, 8) AS rune_liquidity,
  asset_price_usd / rune_usd AS rune_price,
  rune_usd AS rune_price_usd,
  add_liquidity_count,
  add_asset_liquidity_volume / POW(10, 8) AS add_asset_liquidity,
  add_asset_liquidity_volume / POW(10, 8) * asset_usd AS add_asset_liquidity_usd,
  add_rune_liquidity_volume / POW(10, 8) AS add_rune_liquidity,
  add_rune_liquidity_volume / POW(10, 8) * rune_usd AS add_rune_liquidity_usd,
  withdraw_count,
  withdraw_asset_volume / POW(10, 8) AS withdraw_asset_liquditiy,
  withdraw_asset_volume / POW(10, 8) * asset_usd AS withdraw_asset_liquditiy_usd,
  withdraw_rune_volume / POW(10, 8) AS withdraw_rune_liquidity,
  withdraw_rune_volume / POW(10, 8) * rune_usd AS withdraw_rune_liquidity_usd,
  impermanent_loss_protection_paid / POW(10, 8) AS il_protection_paid,
  impermanent_loss_protection_paid / POW(10, 8) * rune_usd AS il_protection_paid_usd,
  average_slip,
  to_asset_average_slip,
  to_rune_average_slip,
  swap_count,
  to_asset_count AS to_asset_swap_count,
  to_rune_count AS to_rune_swap_count,
  swap_volume / POW(10, 8) AS swap_volume_rune,
  swap_volume / POW(10, 8) * rune_usd AS swap_volume_rune_usd,
  to_asset_volume / POW(10, 8) AS to_asset_swap_volume,
  to_rune_volume / POW(10, 8) AS to_rune_swap_volume,
  totalfees AS total_swap_fees_rune,
  totalfees * rune_usd AS total_swap_fees_usd,
  to_asset_fees AS total_asset_swap_fees,
  to_rune_fees AS total_asset_rune_fees,
  unique_member_count,
  unique_swapper_count,
  units AS liquidity_units
FROM {{ ref("thorchain__pool_block_statistics") }} pbs

JOIN daily_rune_price drp 
ON pbs.day = drp.day AND pbs.asset = drp.pool_name

JOIN pool_fees pf 
ON pbs.day = pf.day AND pbs.asset = pf.pool_name