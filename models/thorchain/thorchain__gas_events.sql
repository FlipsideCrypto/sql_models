{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'gas_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.RUNE_E8,
  e.TX_COUNT,
  e.ASSET_E8,
  e.ASSET
FROM {{source('thorchain_midgard', 'gas_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
