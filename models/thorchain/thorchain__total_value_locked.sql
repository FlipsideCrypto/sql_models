{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_timestamp", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'total_value_locked']
  )
}}

SELECT SUM(rune_depth) * 2 AS total_value_pooled
FROM (
  SELECT
    pool_name,
    rune_e8 AS rune_depth,
    asset_e8 AS asset_depth
  FROM (
    SELECT 
      block_id,
      pool_name,
      rune_e8,
      asset_e8,
      MAX(block_id) OVER (PARTITION BY pool_name) AS max_block_id
    FROM {{source('thorchain', 'block_pool_depths')}} 
  )
  WHERE block_id = max_block_id
)