{{ 
  config(
    materialized='table', 
    unique_key='day', 
    tags=['snowflake', 'silver_thorchain', 'daily_earnings']
  )
}}

WITH max_daily_block AS (
  SELECT 
    max(block_id) AS block_id,
    date_trunc('day', block_timestamp) AS day
  FROM {{ ref('silver_thorchain__prices') }}
  {% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  GROUP BY day
),

daily_rune_price AS (
  SELECT
    p.block_id,
    day,
    AVG(rune_usd) AS rune_usd
    FROM {{ ref('silver_thorchain__prices') }} p
  {% if is_incremental() %}
  WHERE day >= getdate() - INTERVAL '5 days'
  {% endif %}
  JOIN max_daily_block mdb WHERE p.block_id = mdb.block_id
  GROUP BY day, p.block_id
)

SELECT
  br.day,
  COALESCE(liquidity_fee, 0) AS liquidity_fees,
  COALESCE(liquidity_fee * rune_usd, 0) AS liquidity_fees_usd,
  block_rewards AS block_rewards,
  block_rewards * rune_usd AS block_rewards_usd,
  COALESCE(earnings, 0) AS total_earnings,
  COALESCE(earnings * rune_usd, 0) AS total_earnings_usd,
  bonding_earnings AS earnings_to_nodes,
  bonding_earnings * rune_usd AS earnings_to_nodes_usd,
  COALESCE(liquidity_earnings, 0) AS earnings_to_pools,
  COALESCE(liquidity_earnings * rune_usd, 0) AS earnings_to_pools_usd,
  avg_node_count
FROM {{ ref('silver_thorchain__block_rewards') }} br

JOIN daily_rune_price drp 
ON br.day = drp.day

