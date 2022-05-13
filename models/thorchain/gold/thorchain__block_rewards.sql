{{ 
  config(
    materialized='table', 
    unique_key='day', 
    tags=['snowflake', 'thorchain', 'block_rewards']
  )
}}

WITH all_block_id AS (
  SELECT DISTINCT
    block_id,
    block_timestamp
  FROM {{ ref('thorchain__block_pool_depths') }} 
  {% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
),

avg_nodes_tbl AS (
  SELECT
    block_id,
    block_timestamp,
    SUM(CASE WHEN current_status = 'Active' THEN 1 WHEN former_status = 'Active' THEN -1 else 0 END) AS delta
  FROM {{ ref('thorchain__update_node_account_status_events') }} 
  {% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  GROUP BY 1,2
),

all_block_with_nodes AS (
  SELECT 
    all_block_id.block_id,
    all_block_id.block_timestamp,
    delta,
    SUM(delta) OVER (ORDER BY all_block_id.block_timestamp ASC) AS avg_nodes
  FROM all_block_id
  {% if is_incremental() %}
  WHERE all_block_id.block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
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
  {% if is_incremental() %}
  WHERE all_block_id.block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  LEFT JOIN avg_nodes_tbl
  ON all_block_id.block_id = avg_nodes_tbl.block_id
),

all_block_with_nodes_date AS (
  SELECT 
    block_timestamp::date AS day,
    AVG(avg_nodes) AS avg_nodes
  FROM all_block_with_nodes
  {% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  GROUP BY 1
),

liquidity_fee_tbl AS (
  SELECT 
    date(block_timestamp) AS day,
    COALESCE(SUM(LIQ_FEE_IN_RUNE_E8), 0) AS liquidity_fee
  FROM {{ ref('silver_thorchain__swap_events') }} 
  {% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  GROUP BY 1 
),

bond_earnings_tbl AS (
  SELECT
    date(block_timestamp) AS day,
    SUM(bond_e8) AS bond_earnings
  FROM {{ ref('silver_thorchain__rewards_events') }} 
  {% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  GROUP BY 1
),

total_pool_rewards_tbl AS (
  SELECT
    date(block_timestamp) AS day,
    SUM(rune_e8) AS total_pool_rewards
  FROM {{ ref('silver_thorchain__rewards_event_entries') }} 
  {% if is_incremental() %}
  WHERE block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  GROUP BY 1
)

SELECT
  all_block_with_nodes_date.day,
  COALESCE((liquidity_fee_tbl.liquidity_fee / POWER(10, 8)), 0) AS liquidity_fee,
  ((COALESCE(total_pool_rewards_tbl.total_pool_rewards, 0) + COALESCE(bond_earnings_tbl.bond_earnings, 0))) / POWER(10, 8) AS block_rewards,
  ((COALESCE(total_pool_rewards_tbl.total_pool_rewards, 0) + COALESCE(liquidity_fee_tbl.liquidity_fee, 0) + COALESCE(bond_earnings_tbl.bond_earnings, 0))) / POWER(10, 8) AS earnings,
  COALESCE((bond_earnings_tbl.bond_earnings / POWER(10, 8)), 0) AS bonding_earnings,
  ((COALESCE(total_pool_rewards_tbl.total_pool_rewards, 0) + COALESCE(liquidity_fee_tbl.liquidity_fee, 0))) / POWER(10, 8) AS liquidity_earnings,
  all_block_with_nodes_date.avg_nodes + 2 AS avg_node_count 
FROM all_block_with_nodes_date

LEFT JOIN liquidity_fee_tbl
ON all_block_with_nodes_date.day = liquidity_fee_tbl.day

LEFT JOIN total_pool_rewards_tbl
ON all_block_with_nodes_date.day = total_pool_rewards_tbl.day

LEFT JOIN bond_earnings_tbl
ON all_block_with_nodes_date.day = bond_earnings_tbl.day