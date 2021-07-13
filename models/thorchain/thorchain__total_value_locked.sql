{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["day"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'total_value_locked']
  )
}}

WITH total_value_bonded_tbl AS (
  SELECT 
    day,
    SUM(abs_rune_amount) AS total_value_bonded 
  FROM (
    SELECT
      day,
      bond_type,
      CASE WHEN bond_type IN ('bond_returned', 'bond_cost') THEN -1 ELSE 1 END AS direction,
      rune_amount,
      rune_amount * direction AS abs_rune_amount
    FROM (
      SELECT
        day,
        bond_type,
        SUM(rune_amount) OVER (PARTITION BY bond_type ORDER BY day ASC) AS rune_amount
      FROM (
        SELECT 
          date(block_timestamp) AS day,
          bond_type, 
          (SUM(e8) / POW(10, 8)) AS rune_amount
        FROM {{source('thorchain', 'bond_events')}} 
        GROUP BY 1,2
      )
      ORDER BY 1 ASC
    )
  )
  GROUP BY 1
  ORDER BY 1 ASC
),
total_value_pooled_tbl AS (
  SELECT 
    day,
    SUM(rune_depth) * 2 / POWER(10, 8) AS total_value_pooled
  FROM (
    SELECT
      day,
      rune_e8 AS rune_depth,
      asset_e8 AS asset_depth
    FROM (
      SELECT 
        date(block_timestamp) AS day,
        block_id,
        pool_name,
        rune_e8,
        asset_e8,
        MAX(block_id) OVER (PARTITION BY pool_name, date(block_timestamp)) AS max_block_id
      FROM {{source('thorchain', 'block_pool_depths')}} 
    )
    WHERE block_id = max_block_id
  )
  GROUP BY 1
  ORDER BY 1 ASC
)

SELECT 
  COALESCE(total_value_bonded_tbl.day, total_value_pooled_tbl.day) AS day,
  total_value_pooled,
  total_value_bonded,
  total_value_pooled + total_value_bonded AS total_value_locked
FROM total_value_bonded_tbl
FULL JOIN total_value_pooled_tbl
ON total_value_bonded_tbl.day = total_value_pooled_tbl.day