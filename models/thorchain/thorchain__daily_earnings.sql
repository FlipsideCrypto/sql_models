{{ 
  config(
    materialized='table', 
    unique_key=["day"], 
    tags=['snowflake', 'thorchain', 'thorchain_daily_earnings']
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
  liquidity_fee AS liquidity_fees,
  liquidity_fee * rune_usd AS liquidity_fees_usd,
  blockrewards AS block_rewards,
  blockrewards * rune_usd AS block_rewards_usd,
  earnings AS total_earnings,
  earnings * rune_usd AS total_earnings_usd,
  bonding_earnings AS earnings_to_nodes,
  bonding_earnings * rune_usd AS earnings_to_nodes_usd,
  liquidityearnings AS earnings_to_pools,
  liquidityearnings * rune_usd AS earnings_to_pools_usd,
  avg_node_count
FROM {{ ref('thorchain__block_rewards') }} br

JOIN daily_rune_price drp 
ON br.day = drp.day

