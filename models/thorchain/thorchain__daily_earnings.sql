{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_timestamp", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'daily_earnings']
  )
}}

WITH max_daily_block AS (
  SELECT 
    max(block_id) AS block_id,
    date_trunc('day', block_timestamp) AS day
  FROM {{ ref('thorchain__prices') }}
  GROUP BY day
),
daily_rune_price AS (
  SELECT
    p.block_id,
    day,
    AVG(rune_usd) AS rune_usd
    FROM {{ ref('thorchain__prices') }} p
  JOIN max_daily_block mdb WHERE p.block_id = mdb.block_id
  GROUP BY day, p.block_id
)
SELECT
  br.day,
  liquidity_fee / POW(10, 8) AS liquidity_fees,
  liquidity_fee / POW(10, 8) * rune_usd AS liquidity_fees_usd,
  blockrewards / POW(10, 8) AS block_rewards,
  blockrewards / POW(10, 8) * rune_usd AS block_rewards_usd,
  earnings / POW(10, 8) AS total_earnings,
  earnings / POW(10, 8) * rune_usd AS total_earnings_usd,
  bonding_earnings / POW(10, 8) AS earnings_to_nodes,
  bonding_earnings / POW(10, 8) * rune_usd AS earnings_to_nodes_usd,
  liquidityearnings / POW(10, 8) AS earnins_to_pools,
  liquidityearnings / POW(10, 8) * rune_usd AS earnins_to_pools_usd,
  avg_node_count
FROM {{ ref('thorchain__block_rewards') }} br
JOIN daily_rune_price drp ON br.day = drp.day