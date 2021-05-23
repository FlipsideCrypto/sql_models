{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'new_node_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.NODE_ADDR as node_address
FROM {{source('thorchain_midgard', 'new_node_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
