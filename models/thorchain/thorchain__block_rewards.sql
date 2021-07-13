{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["day"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'block_rewards']
  )
}}

WITH all_block_id AS (
  SELECT DISTINCT
      block_id,
      block_timestamp
  FROM {{source('thorchain', 'block_pool_depths')}} 
  ORDER BY 1 DESC
),
avg_nodes_tbl AS (
  SELECT
      block_id,
      block_timestamp,
      SUM(CASE WHEN current_status = 'Active' THEN 1 WHEN former_status = 'Active' THEN -1 else 0 END) AS delta
  FROM {{source('thorchain', 'update_node_account_status_events')}} 
  GROUP BY 1,2
  ORDER BY 2 ASC
),
all_block_with_nodes AS (
  SELECT 
    all_block_id.block_id,
    all_block_id.block_timestamp,
    delta,
    SUM(delta) OVER (ORDER BY all_block_id.block_timestamp ASC) AS avg_nodes
  FROM all_block_id
  LEFT JOIN avg_nodes_tbl
  ON all_block_id.block_id = avg_nodes_tbl.block_id
),
all_block_with_nodes AS (
  SELECT 
    all_block_id.block_id,
    all_block_id.block_timestamp,
    delta,
    SUM(delta) OVER (ORDER BY all_block_id.block_timestamp ASC) AS avg_nodes
  FROM all_block_id
  LEFT JOIN avg_nodes_tbl
  ON all_block_id.block_id = avg_nodes_tbl.block_id
),
all_block_with_nodes_date AS (
  SELECT 
    date(block_timestamp) AS day,
    AVG(avg_nodes) AS avg_nodes
  FROM all_block_with_nodes
  GROUP BY 1
  ORDER BY 1 DESC
),
liquidity_fee_tbl AS (
  SELECT 
      date(block_timestamp) AS day,
      SUM(LIQ_FEE_IN_RUNE_E8) AS liquidity_fee
  FROM {{source('thorchain', 'swap_events')}} 
  GROUP BY 1 
  ORDER BY 1 DESC
),
bond_earnings_tbl AS (
  SELECT
      date(block_timestamp) AS day,
      SUM(bond_e8) AS bond_earnings
  FROM {{source('thorchain', 'rewards_events')}} 
  GROUP BY 1
  ORDER BY 1 DESC
),
total_pool_rewards_tbl AS (
  SELECT
      date(block_timestamp) AS day,
      SUM(rune_e8) AS total_pool_rewards
  FROM {{source('thorchain', 'rewards_event_entries')}} 
  GROUP BY 1
  ORDER BY 1 DESC
)
SELECT
    all_block_with_nodes_date.day,
    (liquidity_fee_tbl.liquidity_fee / POWER(10, 8)) AS liquidity_fee,
    ((total_pool_rewards_tbl.total_pool_rewards + bond_earnings_tbl.bond_earnings)) / POWER(10, 8) AS blockRewards,
    ((total_pool_rewards_tbl.total_pool_rewards + liquidity_fee_tbl.liquidity_fee + bond_earnings_tbl.bond_earnings)) / POWER(10, 8) AS earnings,
    (bond_earnings_tbl.bond_earnings / POWER(10, 8)) AS bonding_earnings,
    ((total_pool_rewards_tbl.total_pool_rewards + liquidity_fee_tbl.liquidity_fee)) / POWER(10, 8) AS liquidityEarnings,
    all_block_with_nodes_date.avg_nodes + 2 AS avg_node_count 
FROM all_block_with_nodes_date
LEFT JOIN liquidity_fee_tbl
ON all_block_with_nodes_date.day = liquidity_fee_tbl.day
LEFT JOIN total_pool_rewards_tbl
ON all_block_with_nodes_date.day = total_pool_rewards_tbl.day
LEFT JOIN bond_earnings_tbl
ON all_block_with_nodes_date.day = bond_earnings_tbl.day