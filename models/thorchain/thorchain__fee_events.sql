{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'fee_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.TX as tx_id,
  e.ASSET,
  e.POOL_DEDUCT,
  e.ASSET_E8
FROM {{source('thorchain_midgard', 'fee_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
