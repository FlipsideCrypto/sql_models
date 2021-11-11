{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'block_pool_depths']
  )
}}

SELECT 
  to_timestamp(d.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  d.RUNE_E8,
  d.ASSET_E8,
  d.POOL as pool_name
FROM {{source('thorchain_midgard', 'block_pool_depths')}} d
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = d.BLOCK_TIMESTAMP
WHERE (d._FIVETRAN_DELETED IS NULL OR d._FIVETRAN_DELETED = False)
GROUP BY block_timestamp,block_id,RUNE_E8,ASSET_E8,pool_name