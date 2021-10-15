{{ 
  config(
    materialized='table', 
    unique_key='day', 
    tags=['snowflake', 'thorchain', 'thorchain_total_value_locked']
  )
}}

WITH bond_type_day AS (
  SELECT
    day,
    bond_type,
    SUM(rune_amount) OVER (PARTITION BY bond_type ORDER BY day ASC) AS rune_amount
  FROM (
    SELECT 
      date(block_timestamp) AS day,
      bond_type, 
      (SUM(e8) / POW(10, 8)) AS rune_amount
    FROM {{ ref('thorchain__bond_events') }} 
    GROUP BY 1,2
  )
),

bond_type_day_direction AS (
  SELECT
    day,
    bond_type,
    CASE WHEN bond_type IN ('bond_returned', 'bond_cost') THEN -1 ELSE 1 END AS direction,
    rune_amount,
    rune_amount * direction AS abs_rune_amount
  FROM bond_type_day
),

total_value_bonded_tbl AS (
  SELECT 
    day,
    SUM(abs_rune_amount) AS total_value_bonded 
  FROM bond_type_day_direction
  GROUP BY 1
),

total_pool_depth AS (
  SELECT 
    date(block_timestamp) AS day,
    block_id,
    pool_name,
    rune_e8,
    asset_e8,
    MAX(block_id) OVER (PARTITION BY pool_name, date(block_timestamp)) AS max_block_id
  FROM {{ ref('thorchain__block_pool_depths') }} 
),

total_pool_depth_max AS (
  SELECT
    day,
    rune_e8 AS rune_depth,
    asset_e8 AS asset_depth
  FROM total_pool_depth
  WHERE block_id = max_block_id
),

total_value_pooled_tbl AS (
  SELECT 
    day,
    SUM(rune_depth) * 2 / POWER(10, 8) AS total_value_pooled
  FROM total_pool_depth_max
  GROUP BY 1
)

SELECT 
  COALESCE(total_value_bonded_tbl.day, total_value_pooled_tbl.day) AS day,
  COALESCE(total_value_pooled, 0) AS total_value_pooled,
  COALESCE(SUM(total_value_bonded) OVER (ORDER BY total_value_bonded_tbl.day ASC), 0) AS total_value_bonded,
  COALESCE(total_value_pooled + SUM(total_value_bonded) OVER (ORDER BY total_value_bonded_tbl.day ASC), 0) AS total_value_locked
FROM total_value_bonded_tbl

FULL JOIN total_value_pooled_tbl
ON total_value_bonded_tbl.day = total_value_pooled_tbl.day

ORDER BY 1 DESC